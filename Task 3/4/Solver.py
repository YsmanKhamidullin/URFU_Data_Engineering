import xml.etree.ElementTree
import xml.etree.ElementTree as ET
import os
import pandas as pd

import sub_tasks_solver

xml_folder = './zip_var_96/'
stars_info = []


def get_info(file_path: str):
    tree = ET.parse(file_path)
    root = tree.getroot()
    for clothing_node in root:
        stars_info.append(get_node_info(clothing_node))


def get_node_info(root: xml.etree.ElementTree.Element):
    star_info_dictionary = {}
    for node in root:
        star_info_dictionary[node.tag] = node.text.strip()
    star_info_dictionary['rating'] = float(star_info_dictionary['rating'])
    return star_info_dictionary


for filename in os.listdir(xml_folder):
    if filename.endswith('.xml'):
        file_path = os.path.join(xml_folder, filename)
        get_info(file_path)

df = pd.DataFrame.from_dict(stars_info)
print(df)
sub_tasks_solver.do_info(stars_info, df, 'name', lambda x: x['rating'] >= 4.9, 'rating', 'size')

