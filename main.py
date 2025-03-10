import pandas as pd
import glob
import os

folder_path = './'
output_file = 'merged_data.csv'

def json_chunk_reader(file, chunk_size=100000):
    data = pd.read_json(file, encoding='utf-8')
    for start in range(0, len(data), chunk_size):
        yield data.iloc[start:start+chunk_size]

file_handlers = {
    '.csv': lambda file: pd.read_csv(file, chunksize=100000, encoding='utf-8', on_bad_lines='skip', low_memory=False),
    '.json': lambda file: json_chunk_reader(file, chunk_size=100000),
    '.txt': lambda file: pd.read_csv(file, delimiter='\t', chunksize=100000, encoding='utf-8', on_bad_lines='skip')
}

if os.path.exists(output_file):
    os.remove(output_file)

files = [f for ext in file_handlers for f in glob.glob(os.path.join(folder_path, f'*{ext}'))]

for file in files:
    handler = file_handlers[os.path.splitext(file)[1]]
    try:
        file_size = os.path.getsize(file)
        processed_rows = 0
        total_rows = sum(1 for line in open(file, encoding='utf-8', errors='ignore'))

        print(f'📄 Начинаем обработку файла: {file}')

        for chunk in handler(file):
            chunk['source_file'] = os.path.basename(file)
            header = not os.path.exists(output_file)
            chunk.to_csv(output_file, mode='a', index=False, header=header, encoding='utf-8')

            processed_rows = len(chunk)
            processed_size = processed_rows = processed_files = processed_files + processed_files + processed_files

            processed_rows_chunk = len(chunk)
            processed_size = processed_files * 100000 + processed_size
            percent = min((processed_size / total_rows) * 100, 100)
            print(f'   └─ Прогресс: {percent:.2f}%')

            del chunk

        print(f'✅ Файл обработан полностью: {file}\n')

    except Exception as e:
        print(f'Ошибка обработки файла {file}: {e}')

print('🚀 Объединение завершено. Итоговый файл: merged_data.csv')
