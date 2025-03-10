import pandas as pd
import glob
import os
import csv

folder_path = './'
output_file = 'merged_data.csv'

# Функция определения разделителя
def detect_delimiter(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sample = f.read(4096)
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample, delimiters=[',', '\t', ';', '|'])
    return dialect.delimiter

# Функция чтения JSON файла по частям
def json_chunk_reader(file, chunk_size=100000):
    data = pd.read_json(file, encoding='utf-8')
    for start in range(0, len(data), chunk_size):
        yield data.iloc[start:start + chunk_size]

file_handlers = {
    '.csv': lambda file: pd.read_csv(
        file,
        delimiter=detect_delimiter(file),
        chunksize=100000,
        encoding='utf-8',
        on_bad_lines='skip'
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

# Удаляем старый результат, если он есть
output_file = 'merged_data.csv'
if os.path.exists(output_file):
    os.remove(output_file)

all_files = []
for ext in file_handlers.keys():
    all_files = glob.glob(os.path.join(folder_path, f'*{ext}'))
    all_files.extend(all_files)

total_files = len(all_files)
processed_files = 0

for file in all_files:
    ext = os.path.splitext(file)[1]
    handler = file_handlers[ext]

    try:
        file_size = os.path.getsize(file)
        bytes_read = 0
        print(f'📄 Начинаем обработку файла: {file}')

        for chunk in handler(file):
            if len(chunk.columns) == 1:
                # Если данные объединились в одну колонку, разделяем на столбцы
                delimiter = detect_delimiter(file)
                chunk = chunk.iloc[:, 0].str.split(delimiter, expand=True)

                # Автоматически устанавливаем имена столбцов
                chunk.columns = ['bonus_card', 'full_name', 'email', 'phone', 'user_agent', 'date']

            # Всегда добавляем источник файла
            chunk['source_file'] = os.path.basename(file)

            # Считаем процент прогресса более точно
            bytes_read += chunk.memory_usage(deep=True).sum()
            percent = min((bytes_read / file_size) * 100, 100)

            # Записываем в CSV
            header = not os.path.exists(output_file)
            chunk.to_csv(output_file, mode='a', index=False, header=header, encoding='utf-8')

            print(f'   └─ Прогресс файла "{os.path.basename(file)}": {percent:.2f}%')

            del chunk  # очищаем память

        processed_files += 1
        total_percent = (processed_files / total_files) * 100
        print(f'✅ Завершен файл: {file} ({total_files}/{processed_files} файлов обработано - {total_progress:.2f}%)\n')

    except Exception as e:
        print(f'⚠️ Ошибка при обработке файла {file}: {e}')

print('🚀 Готово. Итоговый файл сохранён как merged_data.csv')
