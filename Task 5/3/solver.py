import json
import pickle

import msgpack
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['local']
collection = db['jobs']


def to_json_output(data: [], file_name: str) -> None:
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=2, ensure_ascii=False)


def append_db():
    with open('task_3_item.text', 'r', encoding='utf-8') as file:
        lines = file.read().split('=====\n')
        for record in lines:
            if record.strip():
                job_data = {}
                fields = record.strip().split('\n')
                for field in fields:
                    key, value = field.split('::')
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                    job_data[key] = value

                collection.insert_one(job_data)


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['local']
collection = db['jobs']


def delete_documents_by_salary_predicate():
    query = {'$or': [{'salary': {'$lt': 25000}}, {'salary': {'$gt': 175000}}]}
    collection.delete_many(query)


def increase_age():
    collection.update_many({}, {'$inc': {'age': 1}})


def raise_salary_for_professions():
    query = {'job': {'$in': ['Бухгалтер', 'Программист']}}
    collection.update_many(query, {'$mul': {'salary': 1.05}})


def raise_salary_for_cities():
    query = {'city': {'$in': ['Скопье', 'Вильнюс']}}
    collection.update_many(query, {'$mul': {'salary': 1.07}})


def raise_salary_complex_predicate():
    query = {
        'city': 'Скопье',
        'job': {'$in': ['Бухгалтер', 'Программист']},
        'age': {'$gte': 30, '$lte': 40}
    }
    collection.update_many(query, {'$mul': {'salary': 1.10}})


def delete_documents_by_random_predicate():
    query = {'city': 'Сеговия'}
    collection.delete_many(query)

# append_db()
delete_documents_by_salary_predicate()
increase_age()
raise_salary_for_professions()
raise_salary_for_cities()
raise_salary_complex_predicate()
delete_documents_by_random_predicate()
