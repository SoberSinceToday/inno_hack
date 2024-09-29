import clickhouse_driver
import pandas as pd
import re
from nltk import edit_distance
from rapidfuzz import fuzz
from urllib3.exceptions import ProtocolError
from http.client import IncompleteRead
import time
import gc


def phone_convert(phone_number: str) -> str:
    res = ''
    for sym in phone_number:
        if sym.isnumeric():
            res += sym
    return res[1:] if len(res) == 11 else None

def check_string_similarity(str1, str2, threshold=0.8) -> bool:
    str1, str2 = str1.lower(), str2.lower()
    if 1 - (edit_distance(str1, str2) / max(len(str1), len(str2))) >= threshold: return True
    if re.search(str1, str2): return True
    if fuzz.partial_ratio(str1, str2) / 100 >= threshold: return True
    return False

def mail_to_string(mail:str) -> str:
    mail = mail.replace("@","").lower()
    if "." in mail:
        mail = mail.split('.')[0]
    return mail

RETRIES_LIMIT = 3

def query_with_retries(client, query, retries=RETRIES_LIMIT):
    for attempt in range(retries):
        try:
            result = client.query(query)
            return result
        except (ProtocolError, IncompleteRead) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(5)  # Ждём перед повторной попыткой для стабильности
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    return None

def load_data_as_polars(table_name, client):
    chunk_size = 500000
    offset = 0
    chunks = []  # Список для хранения DataFrame чанков

    start_time = time.perf_counter()
    print(f"Starting to upload data from {table_name}")

    while True:
        # Запрос с ограничением на размер чанка
        query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
        data, columns = client.execute(query, with_column_types=True)

        # Если данные пустые, выходим из цикла
        if not data:
            break

        # Преобразование текущего чанка в DataFrame
        df_chunk = pd.DataFrame({col[0]: [row[i] for row in data] for i, col in enumerate(columns)})

        # Добавление чанка в список
        chunks.append(df_chunk)

        offset += chunk_size
        if offset%1000000==0:
            print(f'Обработан чанк: {offset}')

    # Объединение всех чанков в один DataFrame
    full_df = pd.concat(chunks)
    print(f"Data upload complete in {round(time.perf_counter()-start_time,1)} seconds")
    print(f"Loaded DataFrame with {full_df.shape[0]} rows and {full_df.shape[1]} columns")
    return full_df

# Вставляем данные пакетами
def insert_data_batches(client, data, batch_size):
    insert_query = "INSERT INTO table_results (id_is1, id_is2, id_is3) VALUES"
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            client.execute(insert_query, batch)
            print(f"Вставлено {len(batch)} записей")
        except Exception as e:
            print(f"Ошибка при вставке партии данных: {e}")