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
    with open('task_2_item.msgpack', 'rb') as file:
        packed_data = file.read()
        unpacked_data = msgpack.unpackb(packed_data)
        collection.insert_many(unpacked_data)


# 	вывод минимальной, средней, максимальной salary
def salary():
    group_rule = dict(_id="salary", min={"$min": "$salary"}, avg={"$avg": "$salary"}, max={"$max": "$salary"})
    q = [{"$group": group_rule}]
    return collection.aggregate(q)


# 	вывод количества данных по представленным профессиям
def job():
    group_rule = dict(_id="$job", count={"$sum": 1})
    sort_rule = dict(count=-1)
    q = [{"$group": group_rule},
         {"$sort": sort_rule}]
    return collection.aggregate(q)


# 	вывод минимальной, средней, максимальной salary по городу
def city_salary():
    group_rule = dict(_id="$city", min={"$min": "$salary"}, avg={"$avg": "$salary"}, max={"$max": "$salary"})
    q = [{"$group": group_rule}]
    return collection.aggregate(q)


# 	вывод минимальной, средней, максимальной salary по профессии
def job_salary():
    group_rule = dict(_id="$job", min={"$min": "$salary"}, avg={"$avg": "$salary"}, max={"$max": "$salary"})
    q = [{"$group": group_rule}]
    return collection.aggregate(q)


# 	вывод минимального, среднего, максимального возраста по городу
def city_age():
    group_rule = dict(_id="$city", min={"$min": "$age"}, avg={"$avg": "$age"}, max={"$max": "$age"})
    q = [{"$group": group_rule}]
    return collection.aggregate(q)


# 	вывод минимального, среднего, максимального возраста по профессии
def job_age():
    group_rule = dict(_id="$job", min={"$min": "$age"}, avg={"$avg": "$age"}, max={"$max": "$age"})
    q = [{"$group": group_rule}]
    return collection.aggregate(q)


# 	вывод максимальной заработной платы при минимальном возрасте
def max_salary_min_age():
    group_rule = dict(_id={"$min": "$age"}, age={"$min": "$age"}, max_salary={"$max": "$salary"})
    q = [{"$group": group_rule}, {
        "$sort": {"age": 1},
    }]
    return collection.aggregate(q)


# 	вывод минимальной заработной платы при максимальной возрасте
def min_salary_max_age():
    group_rule = dict(_id={"$max": "$age"}, salary={"$min": "$salary"}, age={"$max": "$age"})
    q = [{"$group": group_rule}, {
        "$sort": {"age": -1},
    }]
    return collection.aggregate(q)


# 	вывод минимального, среднего, максимального возраста по городу, при условии, что заработная плата больше 50 000,
#       отсортировать вывод по любому полю.
def stats_by_city_gte_salary():
    match_rule = dict(salary={"$gte": 50000})
    group_rule = dict(_id="$city", min_age={"$min": "$age"}, avg_age={"$avg": "$age"}, max_age={"$max": "$age"})
    q = [{"$match": match_rule}, {"$group": group_rule}, {"$sort": {"min_age": 1}}]
    return collection.aggregate(q)


# 	вывод минимальной, средней, максимальной salary в произвольно заданных диапазонах по городу, профессии,
#       и возрасту: 18<age<25 & 50<age<65

def full_salary_stats():
    match_rule = {
        "city": {"$in": ["Москва"]},
        "job": {"$in": ["Программист"]},
        "$or": [{"age": {"$gt": 18, "$lt": 25}},
                {"age": {"$gt": 50, "$lt": 65}}]}
    group_rule = dict(_id="$job",
                      min_salary={"$min": "$salary"},
                      avg_salary={"$avg": "$salary"},
                      max_salary={"$max": "$salary"})
    q = [{"$match": match_rule}, {"$group": group_rule}]

    return collection.aggregate(q)


# 	произвольный запрос с $match, $group, $sort
def custom():
    match_rule = {
        "city": {"$in": ["Москва"]},
        "job": {"$in": ["Программист"]},
        "$or": [{"age": {"$gt": 18, "$lt": 25}},
                {"age": {"$gt": 50, "$lt": 65}}]}
    group_rule = dict(_id="$job",
                      min_salary={"$min": "$salary"},
                      avg_salary={"$avg": "$salary"},
                      max_salary={"$max": "$salary"})
    sort_rule = {"min_salary": 1}
    q = [{"$match": match_rule}, {"$group": group_rule}, {"$sort": sort_rule}]

    return collection.aggregate(q)


# append_db()
to_json_output(list(salary()), "1.json")
to_json_output(list(job()), "2.json")
to_json_output(list(city_salary()), "3.json")
to_json_output(list(job_salary()), "4.json")
to_json_output(list(city_age()), "5.json")
to_json_output(list(job_age()), "6.json")
to_json_output(list(max_salary_min_age()), "7.json")
to_json_output(list(min_salary_max_age()), "8.json")
to_json_output(list(stats_by_city_gte_salary()), "9.json")
to_json_output(list(full_salary_stats()), "10.json")
to_json_output(list(custom()), "11.json")
