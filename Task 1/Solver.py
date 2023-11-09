import math
import pandas as pd


def first_task():
    output_path = "./Output/1/result.txt"
    words_dictionary = get_words_dictionary_by_path("./Input/1/text_1_var_96")
    words_dictionary = dict(sorted(words_dictionary.items(), key=lambda item: item[1], reverse=True))
    open(output_path, "w+").write("")
    for i in words_dictionary.items():
        item = f"{i[0]} : {i[1]}\n"
        print(item)
        open(output_path, 'a+').writelines(item)


def second_task():
    output_path = "./Output/2/result.txt"
    lines_matrix = get_lines_matrix_by_path("./Input/2/text_2_var_96")
    open(output_path, "w+").write("")
    for line in lines_matrix:
        line_sum = 0
        for number in line:
            line_sum += int(number)
        print(line_sum)
        open(output_path, 'a+').writelines(str(line_sum) + "\n")


def thirds_task():
    output_path = "./Output/3/result.txt"
    open(output_path, "w+").write("")
    lines_matrix = get_lines_matrix_by_path("./Input/3/text_3_var_96")
    for line in lines_matrix:
        for i in range(len(line)):
            cur = line[i]
            if cur == "NA":
                prev = int(line[i - 1])
                next = int(line[i + 1])
                line[i] = str((prev + next) / 2)
        for i in range(len(line)):
            cur = line[i]
            # 50+96 is too high
            if math.sqrt(float(cur)) < (50 + 66):
                line[i] = ""
        res_line = list(filter(lambda a: a != '', line))
        for i in res_line:
            open(output_path, 'a+').writelines(i + " ")
        open(output_path, 'a+').write("\n")


def four_task():
    names = ['id', 'name', "surname", 'age', 'salary', 'phone_number']
    input_path = "./Input/4/text_4_var_96"
    data_frame = pd.read_csv(input_path, names=names, encoding="utf-8")

    data_frame['temp_salary'] = list(map(lambda x: int(x[:-1]), data_frame['salary']))
    mean = data_frame['temp_salary'].mean()
    salary_filter = data_frame['temp_salary'] < mean
    data_frame = data_frame[salary_filter]
    data_frame = data_frame.drop([], axis=1)

    ageFilter = data_frame['age'] > 31
    data_frame = data_frame[ageFilter]

    data_frame = data_frame.sort_values(by=['id'])
    data_frame['FullName'] = data_frame['name'].astype(str) + " " + data_frame['surname']
    data_frame = data_frame.drop(['temp_salary', 'phone_number', 'name', 'surname'], axis=1)
    output_path = "./Output/4/result.csv"
    data_frame.to_csv(output_path, index=False, encoding="utf-8")


def five_task():
    # 1. Read all HTML tables from a given URL
    input_path = "./Input/5/text_5_var_96.html"
    tables = pd.read_html(input_path, encoding="utf-8")
    # 2. Write first table, for example, to the CSV file
    output_path = "./Output/5/result.csv"
    tables[0].to_csv(output_path, index= False, encoding="utf-8")


def get_words_dictionary_by_path(path: str) -> dict:
    words_dictionary = {}
    words_matrix = get_lines_matrix_by_path(path)
    for words_line in words_matrix:
        for word in words_line:
            if word in words_dictionary:
                words_dictionary[word] += 1
            else:
                words_dictionary[word] = 1
    return words_dictionary


def get_lines_matrix_by_path(path: str) -> list[list[str]]:
    input_text = open(path).readlines()
    words = []
    for line in input_text:
        parsed_line = (line.strip()
                       .replace(".", " ")
                       .replace(",", " ")
                       .replace("!", " ")
                       .replace("?", " ")
                       .replace("/", " ")
                       .replace("#", " ")
                       .strip()).split(" ")
        words.append(parsed_line)
    return words


if __name__ == '__main__':
    first_task()
    second_task()
    thirds_task()
    four_task()
    five_task()
