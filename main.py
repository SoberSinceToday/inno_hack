import clickhouse_driver
import pandas as pd
import re
from nltk import edit_distance
from rapidfuzz import fuzz
from urllib3.exceptions import ProtocolError
from http.client import IncompleteRead
import time

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

def load_data_as_polars(table_name):
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

client = clickhouse_driver.Client(host='141.8.197.240', port=9000)

df1 = load_data_as_polars('table_dataset1')
df2 = load_data_as_polars('table_dataset2')
df3 = load_data_as_polars('table_dataset3')

start_time = time.perf_counter()
print('Начало обработки данных')

df1['email'] = df1['email'].astype(str)
df1['full_name'] = df1['full_name'].astype(str).apply(lambda x: x.lower())
df1['phone'] = df1['phone'].astype(str).apply(phone_convert)
email_list = []
for row in df1.itertuples():
    email_list.append(mail_to_string(row.email))
df1['email'] = email_list

df2['middle_name'] = df2['middle_name'].astype(str).apply(lambda x: x.lower())
df2['first_name'] = df2['first_name'].astype(str).apply(lambda x: x.lower())
df2['last_name'] = df2['last_name'].astype(str).apply(lambda x: x.lower())
df2['full_name'] = df2.apply(lambda row: " ".join([row['first_name'], row['middle_name'], row['last_name']]), axis=1)
df2 = df2.drop(['first_name', 'middle_name', 'last_name'], axis=1)
df2['phone'] = df2['phone'].astype(str).apply(phone_convert)

df3['email'] = df3['email'].astype(str)
email_list = []
for row in df3.itertuples():
    email_list.append(mail_to_string(row.email))
df3['email'] = email_list
df3['name'] = df3['name'].astype(str).apply(lambda x: x.lower())

combined_df = pd.concat([df1, df2])
value_counts = combined_df['phone'].value_counts()
duplicates = value_counts[value_counts > 1].index.tolist()
repeated_values_df1 = df1.loc[df1['phone'].isin(duplicates)]
repeated_values_df2 = df2.loc[df2['phone'].isin(duplicates)]
grouped_df = repeated_values_df2.groupby('phone')['uid'].apply(list).reset_index()
value_counts = df1['address'].value_counts()
duplicates = value_counts[value_counts > 1].index.tolist()
repeated_values_df1 = df1.loc[df1['address'].isin(duplicates)]
repeated_values_df2 = df2.loc[df2['address'].isin(duplicates)]
repeated_values_df1 = repeated_values_df1[['uid', 'address']]
repeated_values_df2 = repeated_values_df2[['uid', 'address']]

res = pd.DataFrame();
for i1, raw1 in repeated_values_df1.iterrows():
    first_arr_els = [raw1['uid']]
    sec_arr_els = []
    for i2, raw2 in repeated_values_df1.iterrows():
        if raw1['address'] == raw2['address'] and i1 != i2:
            first_arr_els.append(raw2['uid'])
    for i2, raw2 in repeated_values_df2.iterrows():
        if raw1['address'] == raw2['address'] and i1 != i2:
            sec_arr_els.append(raw2['uid'])
    new_row_df = pd.DataFrame([{'id_is1': first_arr_els, 'id_is2': sec_arr_els, 'id_is3': []}])
    res = pd.concat([res, new_row_df], ignore_index=True)
    if len(res) > 10:
        break

print(f"Данные обработаны за {round(time.perf_counter()-start_time,1)} секунд")

# Очистка таблицы перед записью
client.execute('TRUNCATE table_results;')
print(f"Таблица table_results очищена!")

records = res.to_records(index=False)
data_to_insert = list(records)

print(len(data_to_insert))
print(res.head())

# Определяем размер партии (batch size)
batch_size = 1000

# Вызов функции для вставки данных партиями
insert_data_batches(client, data_to_insert, batch_size)
