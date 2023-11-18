import sqlite3
import csv


class DatabaseInitializer:
    def __init__(self, db_name, source: [dict], table_name: str):
        self.db_name = db_name
        self.source = source
        self.table_name = table_name

    def init(self):
        self.drop_table()
        self.create_table()
        self.fill_table()

    def create_table(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        columns_str = ''
        for item in self.source[0].items():
            key = item[0]
            value = item[1]
            if intTryParse(value):
                columns_str += (f',{key} integer')
            else:
                columns_str += (f',{key} TEXT')
        columns_str = columns_str[1:]
        create_table_query = f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {columns_str}
            )
        '''
        cursor.execute(create_table_query)
        conn.commit()
        conn.close()

    def drop_table(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute(f'''DROP TABLE IF EXISTS {self.table_name}''')
        conn.commit()
        conn.close()

    def fill_table(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        insert_query = f'''
            INSERT INTO {self.table_name} ({', '.join(self.source[0])})
            VALUES ({', '.join(['?'] * len(self.source[0]))})
        '''

        for row in self.source:
            values = []
            for column in self.source[0]:
                try:
                    values.append(row[column])
                except:
                    values.append(None)
            #values = tuple(row[column] for column in self.source[0])
            cursor.execute(insert_query, values)

        conn.commit()
        conn.close()


def intTryParse(value):
    try:
        value = int(value)
        return True
    except:
        return False
