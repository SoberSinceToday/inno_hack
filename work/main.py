import clickhouse_connect
import pandas as pd
import re
from nltk import edit_distance
from rapidfuzz import fuzz

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

client = clickhouse_connect.get_client(host='clickhouse123', port=8123, username='default', password='')

df1 = client.query('SELECT * FROM table_dataset1')
df2 = client.query('SELECT * FROM table_dataset2')
df3 = client.query('SELECT * FROM table_dataset3')

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

res = pd.DataFrame()
for i1, raw1 in repeated_values_df1.iterrows():
    first_arr_els = [raw1['uid']]
    sec_arr_els = []
    for i2, raw2 in repeated_values_df1.iterrows():
        if raw1['address'] == raw2['address'] and i1 != i2:
            first_arr_els.append(raw2['uid'])
    for i2, raw2 in repeated_values_df2.iterrows() and i1 != i2:
        if raw1['address'] == raw2['address']:
            sec_arr_els.append(raw2['uid'])
    new_row_df = pd.DataFrame([{'id_is1': first_arr_els, 'id_is2': sec_arr_els, 'id_is3': []}])
    res = pd.concat([res, new_row_df], ignore_index=True)
    if len(res) > 10:
        break

client.insert('table_results', res);
