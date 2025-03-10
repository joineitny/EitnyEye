import pandas as pd
import sqlite3
import json
import glob
import os
import re
import logging
import ijson
import threading
import shutil
import psutil
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Чтение конфигурации из внешнего файла
with open('config.json') as config_file:
    config = json.load(config_file)

THREADS = config['threads']
CHUNKSIZE = config['chunksize']
DATA_FOLDER = config['data_folder']
DB_PATH = config['database_path']

# Настройка логирования
logging.basicConfig(filename='data_processing.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

# Создание резервной копии БД
if os.path.exists(DB_PATH):
    shutil.copy(DB_PATH, DB_PATH + '.backup')
    logging.info("Создана резервная копия базы данных.")

# Создаем подключение к новой базе SQLite
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
db_lock = threading.Lock()

# Создаем итоговую таблицу
cursor.execute('''
CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    age INTEGER,
    email TEXT,
    source_file TEXT
)
''')

# Мониторинг системы
monitoring = True
def monitor_resources():
    while monitoring:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        logging.info(f"CPU: {cpu}%, RAM: {mem}%")

monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
monitor_thread.start()

# Функция предварительной обработки файлов (замена разделителей)
def preprocess_file(file):
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()

    content = re.sub(r'[,:;\t]+', '|', content)

    preprocessed_file = file + '_processed'
    with open(preprocessed_file, 'w', encoding='utf-8') as f:
        f.write(content)
    return preprocessed_file

# Функция обработки CSV файлов
def process_csv(file):
    file = preprocess_file(file)
    df_iter = pd.read_csv(file, delimiter='|', encoding='utf-8', chunksize=CHUNKSIZE)
    df_list = [chunk.assign(source_file=file) for chunk in df_iter]
    return pd.concat(df_list, ignore_index=True)

# Функция обработки JSON файлов с потоковой обработкой
def process_json(file):
    data = []
    with open(file, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, 'item')
        for item in parser:
            data.append(item)
            if len(data) >= CHUNKSIZE:
                break
    df = pd.json_normalize(data)
    df['source_file'] = file
    return df

# Функция обработки TXT файлов
def process_txt(file):
    file = preprocess_file(file)
    df_iter = pd.read_csv(file, delimiter='|', encoding='utf-8', chunksize=CHUNKSIZE)
    df_list = [chunk.assign(source_file=file) for chunk in df_iter]
    return pd.concat(df_list, ignore_index=True)

# Функция обработки SQL файлов
def process_sql(file):
    temp_conn = sqlite3.connect(file)
    df = pd.read_sql_query(f"SELECT * FROM people LIMIT {CHUNKSIZE}", temp_conn)
    df['source_file'] = file
    temp_conn.close()
    return df

# Функция обработки файла с логированием, защитой от дубликатов и мониторингом времени
def process_file(file):
    start_time = time.time()
    filename, ext = os.path.splitext(file)
    try:
        if ext == '.csv':
            df = process_csv(file)
        elif ext == '.json':
            df = process_json(file)
        elif ext == '.txt':
            df = process_txt(file)
        elif ext == '.sql':
            df = process_sql(file)
        else:
            logging.info(f"Пропущен неизвестный формат: {file}")
            return

        df.drop_duplicates(inplace=True)
        with db_lock:
            df.to_sql('people', conn, if_exists='append', index=False)
        elapsed_time = time.time() - start_time
        logging.info(f"Файл {file} обработан за {elapsed_time:.2f} сек.")

    except Exception as e:
        logging.error(f"Ошибка обработки {file}: {e}")

# Запуск мониторинга ресурсов
monitor_thread.start()

# Обрабатываем все файлы параллельно
all_files = glob.glob(f'{DATA_FOLDER}/*')

with ThreadPoolExecutor(max_workers=THREADS) as executor:
    list(tqdm(executor.map(process_file, all_files), total=len(all_files), desc="Processing files"))

# Остановка мониторинга ресурсов
monitoring = False
monitor_thread.join()

# Завершаем работу с базой
conn.commit()
conn.close()

# Очистка временных файлов
for f in glob.glob(f"{DATA_FOLDER}/*_processed"):
    os.remove(f)
logging.info("Все временные файлы очищены.")
logging.info("Обработка завершена успешно!")
