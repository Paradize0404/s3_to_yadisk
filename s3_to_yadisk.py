import boto3
import requests
import os
import re
from botocore.client import Config
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime

# ------------------------------------------------------------------------------
# ⬇️  Настройки «обратной» синхронизации
# TRUE  →  если файла НЕТ на Я‑диске, мы удаляем его копию из S3
# FALSE →  ничего не удаляем
DELETE_MISSING = True
# ------------------------------------------------------------------------------

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

#def file_exists_on_disk(subfolder, filename):
#    url = f"https://webdav.yandex.ru/{quote(DISK_FOLDER + '/' + subfolder + '/' + filename)}"
#    r = requests.head(url, auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD))
#    return r.status_code == 200

def list_files_in_disk_folder(folder):
    url = f"https://webdav.yandex.ru/{quote(DISK_FOLDER + '/' + folder)}"
    r = requests.request(
        "PROPFIND", url,
        auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD),
        headers={"Depth": "1"}
    )
    if r.status_code != 207:
        print(f"⚠️ Не удалось получить список файлов из {folder}: {r.status_code}")
        return set()

    names = set()
    # находим все <d:href>…</d:href> и берём чистое имя файла
    for href in re.findall(r"<d:href>(.*?)</d:href>", r.text, flags=re.IGNORECASE):
        if f"/{folder}/" in href and '.' in href:
            names.add(href.split('/')[-1])        # уже без тега
    return names

existing_cache = {}

def upload_to_disk(local_path, key):
    parts = key.split('/')
    subfolder = parts[1] if len(parts) >= 3 else ''
    filename  = parts[2] if len(parts) >= 3 else parts[-1]

    ensure_folder_exists(subfolder)


    if len(parts) >= 3:
        subfolder = parts[1]
        filename = parts[2]
    else:
        subfolder = ''
        filename = parts[-1]

    ensure_folder_exists(subfolder)



    if filename in existing_cache[subfolder]:
        print(f"⏩ Пропущено (уже есть): {subfolder}/{filename}")
        return

    upload_url = f"https://webdav.yandex.ru/{quote(DISK_FOLDER + '/' + subfolder + '/' + filename)}"
    with open(local_path, 'rb') as f:
        r = requests.put(upload_url, auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD), data=f)

    print(f"⬆️ Загружено: {subfolder}/{filename} → статус: {r.status_code}")

    existing_cache.setdefault(subfolder, set()).add(filename)


def sync():
    print("📂 Загрузка списка всех файлов с диска...")
    existing_cache.clear()
    disk_folders = set()

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # 1. Собираем подпапки invoices/YYYY-MM-DD
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('/'):
            continue
        parts = key.split('/')
        if len(parts) >= 3:
            disk_folders.add(parts[1])

    # 2. Считываем содержимое каждой подпапки с диска
    for folder in disk_folders:
        existing_cache[folder] = list_files_in_disk_folder(folder)

    # 3. Качаем и заливаем то, чего нет на диске
    for obj in response.get('Contents', []):
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
                delete_from_s3(key)

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