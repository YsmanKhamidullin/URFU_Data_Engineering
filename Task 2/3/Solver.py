import json
import numpy as np
import msgpack
import os

with open('products_96.json', 'r') as json_file:
    products_data = json.load(json_file)

aggregate_info = {}

for product in products_data:
    product_name = product['name']
    price = product['price']
    if product_name not in aggregate_info:
        aggregate_info[product_name] = []

    aggregate_info[product_name].append(price)

for product_name in aggregate_info:
    aggregate_info[product_name] = np.array(aggregate_info[product_name])

result = {}
for product_name in aggregate_info:
    prices = aggregate_info[product_name]
    avg_price = np.mean(prices)
    max_price = np.max(prices)
    min_price = np.min(prices)

    result[product_name] = {
        'avg_price': avg_price,
        'max_price': max_price,
        'min_price': min_price
    }

with open('aggregate_info_json.json', 'w') as json_output:
    json.dump(result, json_output)

with open('aggregate_info_msgpack.msgpack', 'wb') as msgpack_output:
    packed_data = msgpack.packb(result)
    msgpack_output.write(packed_data)

json_file_size = os.path.getsize('aggregate_info_json.json')
msgpack_file_size = os.path.getsize('aggregate_info_msgpack.msgpack')

print(f"Размер файла JSON: {json_file_size} байт")
print(f"Размер файла Msgpack: {msgpack_file_size} байт")
