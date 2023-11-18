import sqlite3
import os
import msgpack

import sub_tasks_solver
from database_initializer import DatabaseInitializer


def get_cheap_books():
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    query_1 = f'''
        SELECT {main_table_name}.Title, {table_name}.price
        FROM {main_table_name}
        INNER JOIN {table_name} ON {main_table_name}.Title = {table_name}.Title
        WHERE {table_name}.price < 700 AND {table_name}.place = 'online'
        ORDER BY {table_name}.price;
    '''
    cursor.execute(query_1)
    result_1 = cursor.fetchall()
    print("Результат запроса 1:")
    print(result_1)
    conn.close()
    return result_1


def same_titles():
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    query_2 = f'''
        SELECT {main_table_name}.Title, {table_name}.price
        FROM {main_table_name}, {table_name}
        WHERE {main_table_name}.Title = {table_name}.Title;
    '''
    cursor.execute(query_2)
    result_2 = cursor.fetchall()
    print("\nРезультат запроса 2:")
    print(result_2)
    conn.close()
    return result_2


def get_title_prices():
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    query_3 = f'''
        SELECT {main_table_name}.Title, {table_name}.price
        FROM {main_table_name}, {table_name}
        WHERE {main_table_name}.Title LIKE 'Молот%'
        ORDER BY {table_name}.price DESC
    '''
    cursor.execute(query_3)
    result_3 = cursor.fetchall()
    print("\nРезультат запроса 3:")
    print(result_3)
    conn.close()
    return result_3


if __name__ == '__main__':
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    db_name = os.path.join(parent_directory, '1', 'books_db.db')
    main_table_name = 'books'
    table_name = 'add_books'
    with open('task_2_var_96_subitem.msgpack', 'rb') as file:
        packed_data = file.read()
        unpacked_data = msgpack.unpackb(packed_data)
        parsed_dict = []
        for row in unpacked_data:
            parsed_dict.append(row)
        db_initializer = DatabaseInitializer(source=parsed_dict, db_name=db_name, table_name=table_name)
        db_initializer.init()
        query_1 = get_cheap_books()
        print('\n')
        query_2 = same_titles()
        print('\n')
        query_3 = get_title_prices()

        parsed_dict = []
        for row in get_cheap_books():
            parsed_dict.append({'Title': row[0],'Price': row[1]})
        sub_tasks_solver.create_parsed_json(parsed_dict)

