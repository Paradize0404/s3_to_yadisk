import boto3
import requests
import os
import re
from botocore.client import Config
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime

import psycopg2, psycopg2.extras

pg_conn = psycopg2.connect(
    host     = os.getenv("PGHOST"),
    port     = os.getenv("PGPORT", 5432),
    user     = os.getenv("PGUSER"),
    password = os.getenv("PGPASSWORD"),
    dbname   = os.getenv("PGDATABASE")
)
pg_conn.autocommit = True
cur = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# ------------------------------------------------------------------------------
# ⬇️  Настройки «обратной» синхронизации
# TRUE  →  если файла НЕТ на Я‑диске, мы удаляем его копию из S3
# FALSE →  ничего не удаляем
DELETE_MISSING = True
# ------------------------------------------------------------------------------

def db_file_exists(key: str) -> bool:
    cur.execute("SELECT 1 FROM ydisk_files WHERE key=%s AND in_disk", (key,))
    return cur.fetchone() is not None

def db_mark_present(key: str, subfolder: str, filename: str):
    cur.execute(
        """INSERT INTO ydisk_files(key,subfolder,filename,in_disk)
           VALUES (%s,%s,%s,true)
           ON CONFLICT (key) DO UPDATE
             SET in_disk = true, updated_at = now()""",
        (key, subfolder, filename)
    )

def db_mark_deleted(key: str):
    cur.execute("UPDATE ydisk_files SET in_disk=false, updated_at=now() WHERE key=%s", (key,))




def delete_from_s3(key: str):
    """Удалить объект из S3 и вывести лог."""
    s3.delete_object(Bucket=bucket_name, Key=key)
    print(f"🗑️  Удалено из S3: {key}")

# 🔐 Доступ к S3
s3 = boto3.client(
    's3',
    endpoint_url='https://storage.yandexcloud.net',
    region_name=os.getenv("YANDEX_REGION", "ru-central1"),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    config=Config(signature_version='s3v4')
)

bucket_name = 'yolobot'
prefix = 'invoices/'
local_tmp = '/tmp'

# 🔐 Доступ к Яндекс.Диску (через WebDAV)
YANDEX_LOGIN = os.getenv('YANDEX_LOGIN')
YANDEX_APP_PASSWORD = os.getenv('YANDEX_APP_PASSWORD')
DISK_FOLDER = 'Накладные'


def is_already_uploaded(key: str) -> bool:
    parts = key.split('/')
    if len(parts) < 3:
        return False
    subfolder, filename = parts[1], parts[2]
    return filename in existing_cache.get(subfolder, set())




def ensure_folder_exists(folder_name):
    url = f"https://webdav.yandex.ru/{quote(DISK_FOLDER + '/' + folder_name)}"
    r = requests.request("MKCOL", url, auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD))
    if r.status_code not in (201, 405):  # 201 = создано, 405 = уже существует
        print(f"⚠️ Не удалось создать папку {folder_name}: {r.status_code} {r.text}")

def disk_file_exists(subfolder: str, filename: str) -> bool:
    """
    Быстрый HEAD-запрос к WebDAV. Возвращает True, если файл
    реально лежит на Я.Диске (а не просто «записан в БД»).
    """
    url = f"https://webdav.yandex.ru/{quote(DISK_FOLDER + '/' + subfolder + '/' + filename)}"
    r = requests.head(url, auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD))
    return r.status_code == 200        # 200 – OK, всё в порядке



existing_cache = {}

def upload_to_disk(local_path, key):
    parts = key.split('/')
    subfolder = parts[1] if len(parts) >= 3 else ''
    filename  = parts[2] if len(parts) >= 3 else parts[-1]

    ensure_folder_exists(subfolder)





    if filename in existing_cache.get(subfolder, set()):
        print(f"⏩ Пропущено (уже есть): {subfolder}/{filename}")
        return

    upload_url = f"https://webdav.yandex.ru/{quote(DISK_FOLDER + '/' + subfolder + '/' + filename)}"
    with open(local_path, 'rb') as f:
        r = requests.put(upload_url, auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD), data=f)



    print(f"⬆️ Загружено: {subfolder}/{filename} → статус: {r.status_code}")
    db_mark_present(key, subfolder, filename)
    existing_cache.setdefault(subfolder, set()).add(filename)   # 🆕 фиксируем в кэше



def sync():
    print(f"[{datetime.now():%H:%M:%S}] 📂 Загрузка списка всех файлов с диска...")

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    existing_cache.clear()
    cur.execute("SELECT subfolder, filename FROM ydisk_files WHERE in_disk")
    for subfolder, filename in cur.fetchall():
        existing_cache.setdefault(subfolder, set()).add(filename)

    # 3. Качаем и заливаем то, чего нет на диске
    for obj in response.get('Contents') or []:
        key = obj['Key']
        if key.endswith('/'):
            continue
        if not is_already_uploaded(key):
            filename   = key.split('/')[-1]
            local_path = os.path.join(local_tmp, filename)
            s3.download_file(bucket_name, key, local_path)
            print(f'📥 Скачано: {key}')
            upload_to_disk(local_path, key)
        else:
            print(f'⏭️ Пропущено (уже есть): {key}')

    # 4. Удаляем из S3 объекты, отсутствующие на диске
    if DELETE_MISSING:
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue
            parts = key.split('/')
            if len(parts) < 3:
                continue
            subfolder, filename = parts[1], parts[2]
            if filename not in existing_cache.get(subfolder, set()):
                # убеждаемся, что файла точно нет на Диске
                if not disk_file_exists(subfolder, filename):
                    delete_from_s3(key)
                    db_mark_deleted(key)

print("🧪 KEY:", os.getenv('AWS_ACCESS_KEY_ID'))
print("🧪 SECRET:", os.getenv('AWS_SECRET_ACCESS_KEY')[:5], '...')


if __name__ == '__main__':
    while True:
        now = datetime.now()
        hour = now.hour

        if 8 <= hour < 17:
            try:
                print(f"🚀 [{now.strftime('%H:%M:%S')}] Запуск синхронизации...")
                sync()
                print(f"✅ [{now.strftime('%H:%M:%S')}] Синхронизация завершена.")
            except Exception as e:
                print(f"❌ [{now.strftime('%H:%M:%S')}] ОШИБКА:", e)
        else:
            print(f"⏸️ Сейчас {hour}:00 — вне рабочего интервала (8–17)")

        time.sleep(300)  # 5 минут