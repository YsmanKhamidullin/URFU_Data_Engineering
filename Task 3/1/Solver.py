import os
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
import sub_tasks_solver

html_folder = './zip_var_96/'
objects_data = []


def get_by_pattern(pattern, soup):
    pattern = re.compile(pattern)
    matches = pattern.findall(soup.text.strip())
    return matches[0] if matches else None


def parse_html(file_path):
    with open(file_path, 'r', encoding='utf-8') as html_file:
        soup = BeautifulSoup(html_file, 'html.parser')
        string_reg_pattern = '\s*(\w+)'
        digit_reg_pattern = '\s*([\d.])+'
        obj_data = {
            'Тип': get_by_pattern(r'Тип:' + string_reg_pattern, soup),
            'Турнир': get_by_pattern(r'Турнир:' + string_reg_pattern, soup),
            'Город': get_by_pattern(r'Город:' + string_reg_pattern, soup),
            'Количество туров': get_by_pattern(r'Количество туров:' + string_reg_pattern, soup),
            'Контроль времени': get_by_pattern(r'Контроль времени:' + string_reg_pattern, soup),
            'Минимальный рейтинг': get_by_pattern(r'Минимальный рейтинг для участия:' + string_reg_pattern, soup),
            'Изображение': soup.find('img')['src'],
            'Рейтинг': float(get_by_pattern(r'Рейтинг:' + digit_reg_pattern, soup)),
            'Просмотры': get_by_pattern(r'Просмотры:' + string_reg_pattern, soup)
        }

    return obj_data


for filename in os.listdir(html_folder):
    if filename.endswith('.html'):
        file_path = os.path.join(html_folder, filename)
        obj_data = parse_html(file_path)
        objects_data.append(obj_data)

df = pd.DataFrame(objects_data)

sub_tasks_solver.do_info(objects_data, df, 'Турнир', lambda x: x['Тип'] == 'Swiss', 'Рейтинг', 'Тип')
