import csv

from database_initializer import DatabaseInitializer
import DB_Filter_Sorting

def parse_line(line):
    pairs = [pair.split('::') for pair in line.strip().split('\n')]
    return {key: value for key, value in pairs}


if __name__ == '__main__':
    db_name = './products_db.db'
    table_name = 'products'

    with open('task_4_var_96_product_data.text', 'r', encoding='utf-8') as file:
        product_blocks = file.read().split('=====\n')
        parsed_dict = [parse_line(block) for block in product_blocks if block]
        db_initializer = DatabaseInitializer(source=parsed_dict, db_name=db_name, table_name=table_name)
        db_initializer.init()

        keys = parsed_dict[0].keys()
        DB_Filter_Sorting.sort_numeric_field_to_json('quantity', 106, db_name, table_name, keys)
        DB_Filter_Sorting.numeric_stat('quantity', db_name, table_name)
        DB_Filter_Sorting.frequency_stat('quantity', db_name, table_name)
        DB_Filter_Sorting.filter_sorted('quantity', 1000, 106, db_name, table_name, keys)