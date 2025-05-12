import boto3
import requests
import os
from botocore.client import Config
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
import time


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

from urllib.parse import quote

def is_already_uploaded(key):
    parts = key.split('/')
    if len(parts) < 3:
        return False
    subfolder = parts[1]
    filename = parts[2]

    

    return filename in existing_cache[subfolder]




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
    r = requests.request("PROPFIND", url, auth=(YANDEX_LOGIN, YANDEX_APP_PASSWORD), headers={"Depth": "1"})
    if r.status_code != 207:
        print(f"⚠️ Не удалось получить список файлов из {folder}: {r.status_code}")
        return set()
    
    # Извлекаем имена файлов из ответа
    return set(line.split('/')[-1] for line in r.text.split('<d:href>') if folder in line and '.' in line)

existing_cache = {}

def upload_to_disk(local_path, key):
    parts = key.split('/')
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

    existing_cache[subfolder].add(filename)

def sync():

    print("📂 Загрузка списка всех файлов с диска...")
    existing_cache.clear()
    disk_folders = set()
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Собираем все уникальные папки
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('/'):
            continue
        parts = key.split('/')
        if len(parts) >= 3:
            disk_folders.add(parts[1])

    # Загружаем список файлов по каждой папке один раз
    for folder in disk_folders:
        existing_cache[folder] = list_files_in_disk_folder(folder)



    files = []

    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('/'):
            continue
        filename = key.split('/')[-1]
        local_path = os.path.join(local_tmp, filename)
        # Проверяем: есть ли уже на Диске
        # скачиваем, только если ещё нет
        if not is_already_uploaded(key):
            s3.download_file(bucket_name, key, local_path)
            print(f'📥 Скачано: {key}')
            upload_to_disk(local_path, key)
        else:
            print(f'⏭️ Пропущено (уже есть): {key}')

        

    def process_file(file):
        local_path, key = file
        upload_to_disk(local_path, key)

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_file, files)

print("🧪 KEY:", os.getenv('AWS_ACCESS_KEY_ID'))
print("🧪 SECRET:", os.getenv('AWS_SECRET_ACCESS_KEY')[:5], '...')


from datetime import datetime

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