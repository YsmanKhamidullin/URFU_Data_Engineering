from pymongo import MongoClient
from faker import Faker
import random


def connect_to_mongodb():
    client = MongoClient('localhost', 27017)
    db = client['local']
    return db


def generate_fake_authors(num_authors):
    fake = Faker()
    authors_data = []
    for i in range(1, num_authors + 1):
        author = {
            "id": i,
            "name": fake.name(),
            "birth_year": fake.random_int(1800, 2000),
            "country": fake.country()
        }
        authors_data.append(author)
    return authors_data


def generate_fake_books(num_books, num_authors):
    fake = Faker()
    books_data = []
    for i in range(1, num_books + 1):
        book = {
            "title": fake.catch_phrase(),
            "author_id": random.randint(1, num_authors),
            "genre": fake.word(),
            "price": round(random.uniform(10, 50), 2),
            "publish_year": fake.random_int(1800, 2023)
        }
        books_data.append(book)
    return books_data


def insert_data_into_collections(db, authors_data, books_data):
    db.authors.insert_many(authors_data)
    db['books'].insert_many(books_data)


def query(db):
    query_1_1 = db['books'].find().sort("title").limit(3)

    query_1_2 = db['books'].find({"genre": "Classics", "price": {"$lt": 25}})

    query_1_3 = db.books.aggregate([
        {"$lookup": {"from": "authors", "localField": "author_id", "foreignField": "id", "as": "author_info"}},
        {"$match": {"author_info.name": "J.D. Salinger"}},
        {"$project": {"title": 1, "_id": 0}}
    ])
    avg_price = db.books.aggregate([{"$group": {"_id": None, "avg": {"$avg": "$price"}}}]).next()["avg"]

    query_1_4 = db.books.find({"price": {"$gt": avg_price}})

    query_1_5 = db.books.find({"publish_year": {"$gt": 2000}})

    query_2_1 = db.books.aggregate([{"$group": {"_id": None, "avgPrice": {"$avg": "$price"}}}])

    query_2_2 = db.books.aggregate([{"$group": {"_id": "$genre", "count": {"$sum": 1}}}])

    query_2_3 = db.books.aggregate([
        {"$lookup": {"from": "authors", "localField": "author_id", "foreignField": "id", "as": "author_info"}},
        {"$group": {"_id": "$author_info.country", "maxPrice": {"$max": "$price"}}}
    ])

    query_3_1 = db.books.delete_many({"price": {"$lt": 10}})

    query_3_2 = db.books.update_many({}, {"$inc": {"publish_year": 1}})

    query_3_3 = db.books.update_many({"genre": "Fiction"}, {"$mul": {"price": 1.05}})


def main():
    db = connect_to_mongodb()
    num_authors = 10
    num_books = 50
    authors_data = generate_fake_authors(num_authors)
    books_data = generate_fake_books(num_books, num_authors)
    insert_data_into_collections(db, authors_data, books_data)

    books_data = [
        {"title": "The Catcher in the Rye", "author_id": 1, "genre": "Fiction", "price": 25.99, "publish_year": 1951},
    ]

    authors_data = [
        {"id": 1, "name": "J.D. Salinger", "birth_year": 1919, "country": "USA"},
    ]

    db['books'].insert_many(books_data)
    db.authors.insert_many(authors_data)
    query(db)

    if __name__ == "__main__":
        main()
