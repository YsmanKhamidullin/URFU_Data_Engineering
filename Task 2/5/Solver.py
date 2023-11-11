import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq
import os

df = pd.read_csv('titanic.csv')
selected_fields = df[['Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare']]
selected_fields.loc[:, ['Age', 'Fare']] = selected_fields[['Age', 'Fare']].fillna(0)

numeric_stats = selected_fields.describe()
text_frequency = selected_fields['Sex'].value_counts().to_dict()
result_json = {
    'numeric_stats': numeric_stats.to_dict(),
    'text_frequency': text_frequency
}

with open('analysis_results.json', 'w') as json_file:
    json.dump(result_json, json_file)

selected_fields.to_csv('selected_data.csv', index=False)
selected_fields.to_json('selected_data.json', orient='records')
selected_fields.to_pickle('selected_data.pkl')
arrow_table = pa.Table.from_pandas(selected_fields)
pq.write_table(arrow_table, 'selected_data.parquet')

csv_file_size = os.path.getsize('selected_data.csv')
json_file_size = os.path.getsize('selected_data.json')
parquet_file_size = os.path.getsize('selected_data.parquet')
pkl_file_size = os.path.getsize('selected_data.pkl')

print(f"Размер файла CSV: {csv_file_size} байт")
print(f"Размер файла JSON: {json_file_size} байт")
print(f"Размер файла Parquet: {parquet_file_size} байт")
print(f"Размер файла Pickle: {pkl_file_size} байт")
