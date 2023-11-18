import sqlite3
import csv
import json
from database_initializer import DatabaseInitializer


def sort_numeric_field_to_json(int_field: str, limit: int, database_name: str, table_name: str, keys: []):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ORDER BY {int_field} LIMIT {limit}')
    rows = cursor.fetchall()
    result_list = []
    for row in rows:
        result_list.append(dict(zip(keys, row)))

    result_json = json.dumps(result_list, ensure_ascii=False)

    with open('output_sorted.json', 'w', encoding='utf-8') as json_file:
        json_file.write(result_json)

    conn.close()


def numeric_stat(numeric_field: str, database_path: str, table: str):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        f'SELECT SUM({numeric_field}), MIN({numeric_field}), MAX({numeric_field}), AVG({numeric_field}) FROM {table}')
    result = cursor.fetchone()

    print(f'Sum: {result[0]}, Min: {result[1]}, Max: {result[2]}, Avg: {result[3]}')

    conn.close()


def frequency_stat(categorical_field: str, database_path: str, table: str):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(f'SELECT {categorical_field}, COUNT(*) FROM {table} GROUP BY {categorical_field}')
    result = cursor.fetchall()
    result.sort(key=lambda x: x[1], reverse=True)
    for row in result:
        print(f'{row[0]}: {row[1]}')

    conn.close()


def filter_sorted(field: str, predicate: int, limit: int, database_path: str, table: str, keys: []):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {table} WHERE {field} > {predicate} ORDER BY {field} LIMIT {limit}', )
    rows = cursor.fetchall()
    result_list = []
    for row in rows:
        result_list.append(dict(zip(keys, row)))

    result_json = json.dumps(result_list, ensure_ascii=False)

    with open('output_filtered.json', 'w', encoding='utf-8') as json_file:
        json_file.write(result_json)

    conn.close()


if __name__ == '__main__':
    db_name = './books_db.db'
    table_name = 'books'
    with open('task_1_var_96_item.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        parsed_dict = []
        for row in csv_reader:
            parsed_dict.append(row)
        db_initializer = DatabaseInitializer(source=parsed_dict, db_name=db_name, table_name=table_name)
        db_initializer.init()

        keys = parsed_dict[0].keys()
        sort_numeric_field_to_json('views', 106, db_name, table_name, keys)
        numeric_stat('pages', db_name, table_name)
        frequency_stat('genre', db_name, table_name)
        filter_sorted('published_year', 1999, 106, db_name, table_name, keys)
