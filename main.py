import clickhouse_driver
import pandas as pd
import re
from nltk import edit_distance
from rapidfuzz import fuzz
from urllib3.exceptions import ProtocolError
from http.client import IncompleteRead
import time
import gc
from functions import *




client = clickhouse_driver.Client(host='141.8.197.240', port=9000)

df1 = load_data_as_polars('table_dataset1', client=client)
df2 = load_data_as_polars('table_dataset2', client=client)
df3 = load_data_as_polars('table_dataset3', client=client)

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

gc.collect()
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
gc.collect()

repeated_values_df3 = df3.loc[df3['email'].isin(duplicates)]
repeated_values_df3 = repeated_values_df3[['uid', 'email']]
grouped_df = repeated_values_df3.groupby('email')['uid'].apply(list).reset_index()
value_counts = df3['email'].value_counts()
duplicates = value_counts[value_counts > 1].index.tolist()
repeated_values_df1 = df1.loc[df1['email'].isin(duplicates)]
repeated_values_df3 = df3.loc[df3['email'].isin(duplicates)]
repeated_values_df1 = repeated_values_df1[['uid', 'email']]
repeated_values_df3 = repeated_values_df3[['uid', 'email']]
repeated_values_df3.head()
for i1, raw1 in repeated_values_df1.iterrows():
    first_arr_els = [raw1['uid']]
    sec_arr_els = []
    for i2, raw2 in repeated_values_df1.iterrows():
        if raw1['email'] == raw2['email'] and i1 != i2:
            first_arr_els.append(raw2['uid'])
    for i2, raw2 in repeated_values_df3.iterrows():
        if raw1['email'] == raw2['email'] and i1 != i2:
            sec_arr_els.append(raw2['uid'])
    new_row_df = pd.DataFrame([{'id_is1': first_arr_els, 'id_is2': [], 'id_is3': sec_arr_els}])
    res = pd.concat([res, new_row_df], ignore_index=True) 
    print(res)
    if len(res) > 20:
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
