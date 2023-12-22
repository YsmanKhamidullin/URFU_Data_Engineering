import json
import pickle
from pymongo import MongoClient

collection_name = 'jobs'
client = MongoClient('localhost', 27017)
db = client['local']
collection = db[collection_name]


def append_db():
    with open('task_1_item.pkl', 'rb') as f:
        jobs = pickle.load(f, encoding='utf-8')
        collection.insert_many(jobs)


def to_json_output(data: [], file_name: str) -> None:
    i = 0
    for d in data:
        d['_id'] = i
        i += 1
    result_json = json.dumps(data, ensure_ascii=False, indent=2)
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json_file.write(result_json)


# вывод первых 10 записей, отсортированных по убыванию по полю salary
def find_first_sorted():
    return collection.find().sort({'salary': -1}).limit(10)


# вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортировать по убыванию по полю salary
def limited_filter_sort():
    predicate = dict(age={'$lt': 30})
    return collection.find(predicate).sort({'salary': -1}).limit(15)


# вывод первых 10 записей, отфильтрованных по сложному предикату: (записи только из произвольного города,
# записи только из трех произвольно взятых профессий), отсортировать по возрастанию по полю age
def filter_hard_predicate():
    predicate = dict(city='Москва',
                     job={'$in': ['Архитектор', 'Повар', 'Строитель']})
    return collection.find(predicate).sort({'age': 1}).limit(10)


# вывод, получаемых в результате следующей фильтрации (age в произвольном диапазоне,
# year в [2019,2022], 50 000 < salary <= 75 000 || 125 000 < salary < 150 000).
def mega_filter():
    doc_count = collection.count_documents({
        "age": {"$gte": 18, "$lte": 24},
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]
    })
    data = {"count": doc_count}
    with open("4_custom_filter.json", 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file)
    return data


# append_db()
result = find_first_sorted()
to_json_output(list(result), "1_sorted.json")
result = limited_filter_sort()
to_json_output(list(result), "2_filtered.json")
result = filter_hard_predicate()
to_json_output(list(result), "3_filtered_hard_predicate.json")
mega_filter()
