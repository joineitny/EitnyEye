import pandas as pd
import glob
import os
import csv

folder_path = './'
output_file = 'merged_data.csv'

def detect_delimiter(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        header = file.readline()
        sniffer = csv.Sniffer()
        dialect = sniffer = csv.Sniffer().sniff(header, delimiters=[',', ';', '\t', '|'])
        return dialect.delimiter

def json_chunk_reader(file, chunk_size=100000):
    data = pd.read_json(file, encoding='utf-8')
    for start in range(0, len(data), chunk_size):
        yield data.iloc[start:start+chunk_size]

file_handlers = {
    '.csv': lambda file: pd.read_csv(
        file, 
        delimiter=detect_delimiter(file),
        chunksize=100000,
        encoding='utf-8',
        on_bad_lines='skip',
        low_memory=False
    ),
    '.json': lambda file: json_chunk_reader(file, chunk_size=100000),
    '.txt': lambda file: pd.read_csv(
        file, 
        delimiter=detect_delimiter(file), 
        chunksize=100000, 
        encoding='utf-8', 
        on_bad_lines='skip'
    )
}

if os.path.exists(output_file):
    os.remove(output_file)

files = [f for ext in file_handlers.keys() for f in glob.glob(os.path.join(folder_path, f'*{ext}'))]

processed_files = 0

for file in files:
    ext = os.path.splitext(file)[1]
    handler = file_handlers[ext]

    try:
        print(f'📄 Начинаем обработку файла: {file}')
        
        total_size = os.path.getsize(file)
        processed_size = 0

        for chunk in handler(file):
            # Автоматическое исправление столбцов при их неправильном объединении
            if len(chunk.columns) == 1:
                # Попытка автоматического разделения на столбцы, если исходно только один столбец
                sample = chunk.iloc[0, 0]
                detected_delim = ',' if ',' in sample else '\t'
                chunk = chunk.iloc[:, 0].str.split(detect_delimiter(file), expand=True)

                # Автоматически определим названия столбцов
                chunk.columns = ['bonus_card', 'full_name', 'email', 'phone', 'user_agent', 'date', 'source_file']

            else:
                chunk['source_file'] = os.path.basename(file)

            header = not os.path.exists(output_file)
            chunk.to_csv(output_file, mode='a', index=False, header=header, encoding='utf-8')

            processed_size += len(chunk.to_csv(index=False).encode('utf-8'))
            percent = min((processed_size / total_size) * 100, 100)
            print(f'   └─ Прогресс: {percent:.2f}%')

            del chunk

        print(f'✅ Завершен файл: {file}')

print('🚀 Готово. Результат: merged_data.csv')
