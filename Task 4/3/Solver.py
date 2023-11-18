import sqlite3
import csv
import json

import msgpack

from database_initializer import DatabaseInitializer
import DB_Filter_Sorting

if __name__ == '__main__':
    db_name = './artist_db.db'
    first_table = 'artists'
    with open('task_3_var_96_part_2.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        parsed_dict = []
        for row in csv_reader:
            parsed_dict.append(row)
        db_initializer = DatabaseInitializer(source=parsed_dict, db_name=db_name, table_name=first_table)
        db_initializer.init()

    second_table = 'artists_part_2'
    with open('task_3_var_96_part_1.msgpack', 'rb') as file:
        packed_data = file.read()
        unpacked_data = msgpack.unpackb(packed_data)
        parsed_dict = []
        for row in unpacked_data:
            parsed_dict.append(row)
        db_initializer = DatabaseInitializer(source=parsed_dict, db_name=db_name, table_name=second_table)
        db_initializer.init()

    merged_table = 'artists_combined'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    queue = f'''
        CREATE TABLE IF NOT EXISTS {merged_table} AS
        SELECT * FROM {first_table}
        INNER JOIN {second_table} ON {first_table}.song = {second_table}.song AND {first_table}.artist = {second_table}.artist;
    '''
    cursor.execute(queue)
    conn.commit()
    conn.close()

    keys = ['artist',
            'song',
            'duration_ms',
            'year',
            'tempo',
            'genre',
            'energy',
            'key',
            'loudness',
            'artist:1',
            'song:1',
            'duration_ms:1',
            'year:1',
            'tempo:1',
            'genre:1',
            'mode',
            'speechiness',
            'acousticness',
            'instrumentalness']

    DB_Filter_Sorting.sort_numeric_field_to_json('year', 106, db_name, merged_table, keys)
    DB_Filter_Sorting.numeric_stat('year', db_name, merged_table)
    DB_Filter_Sorting.frequency_stat('year', db_name, merged_table)
    DB_Filter_Sorting.filter_sorted('year', 1999, 106, db_name, merged_table, keys)
