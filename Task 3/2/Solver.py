import os
import json
import re
import pandas as pd
from bs4 import BeautifulSoup

import sub_tasks_solver

html_folder = './zip_var_96/'
objects_data = []


def str_to_int(source: str):
    return int(re.search(r'\d+', source).group())


def parse_html(file_path):
    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
        product_item = soup.find_all('div', class_='pad')

        for item in product_item:
            result_dict = {
                'Name': item.find('span').text.strip(),
                'Url': item.find_all('a')[1]['href'],
                'Image': item.find('img')['src'],
                'Price': str_to_int(item.find('price').text.replace(' ', '')),
                'Bonus': str_to_int(item.find('strong').text)
            }
            characteristics = item.find('ul').find_all('li')
            result_dict |= {li['type']: li.get_text(strip=True) for li in characteristics}
            objects_data.append(result_dict)


for filename in os.listdir(html_folder):
    if filename.endswith('.html'):
        file_path = os.path.join(html_folder, filename)
        parse_html(file_path)

df = pd.DataFrame(objects_data)
sub_tasks_solver.do_info(objects_data, df, 'Bonus', lambda x: x['Price'] <= 19000, 'Bonus', 'ram')
