import xml.etree.ElementTree as ET
import os
import pandas as pd

import sub_tasks_solver

xml_folder = './zip_var_96/'
stars_info = []


def get_info(file_path: str):
    tree = ET.parse(file_path)
    root = tree.getroot()
    star_info_dictionary = {}
    for node in root:
        star_info_dictionary[node.tag] = node.text.strip()
    star_info_dictionary['radius'] = float(star_info_dictionary['radius'])
    return star_info_dictionary


for filename in os.listdir(xml_folder):
    if filename.endswith('.xml'):
        file_path = os.path.join(xml_folder, filename)
        stars_info.append(get_info(file_path))

df = pd.DataFrame.from_dict(stars_info)
print(df)
sub_tasks_solver.do_info(stars_info, df, 'name', lambda x: x['constellation'] == 'Рак', 'radius', 'constellation')
