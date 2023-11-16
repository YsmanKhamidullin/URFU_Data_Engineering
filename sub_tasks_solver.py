import pandas as pd
import json


def do_info(objects_data: [], source_df: pd.DataFrame, sort_field: str, filter_condition, stats_field: str, frequency_field: str):
    create_parsed_json(objects_data)
    sort_fields(sort_field, source_df)
    filter_fields(filter_condition, source_df)
    get_numeric_stats(source_df, stats_field)
    get_marks_frequency(frequency_field, source_df)


def create_parsed_json(objects_data: []):
    with open('parsed_data.json', 'w', encoding='utf-8') as json_file:
        json.dump(objects_data, json_file, ensure_ascii=False, indent=2)


def sort_fields(sort_field, source_df):
    print("DataFrame:")
    print(source_df)
    sorted_df = source_df.sort_values(by=sort_field)
    print(f"Отсортированные значения по полю {sort_field}:")
    print(sorted_df)


def filter_fields(filter_condition, source_df):
    filtered_df = source_df[filter_condition(source_df)]
    print(f"\nОтфильтрованные значения:")
    print(filtered_df)
    with open('filtered_data.csv', 'w', encoding='utf-8') as json_file:
        json_file.write(filtered_df.to_csv(index=False, encoding='utf-8'))


def get_numeric_stats(source_df, stats_field):
    numeric_stats = source_df[stats_field].describe()
    print(f"\nСтатистические характеристики поля {stats_field}:")
    print(numeric_stats)
    numeric_sum = source_df[stats_field].sum()
    print(f"\nСумма поля {stats_field}:")
    print(numeric_sum)


def get_marks_frequency(frequency_field, source_df):
    text_frequency = source_df[frequency_field].value_counts().to_dict()
    print(f"\nЧастота меток поля {frequency_field}:")
    print(text_frequency)
