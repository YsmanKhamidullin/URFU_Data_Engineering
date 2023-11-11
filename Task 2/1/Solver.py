import numpy as np
import json

matrix = np.load('matrix_96.npy')

total_sum = np.sum(matrix)
total_avr = np.mean(matrix)

sum_main_diagonal = np.trace(matrix)
avr_main_diagonal = np.trace(matrix) / matrix.shape[0]

sum_secondary_diagonal = np.trace(np.fliplr(matrix))
avr_secondary_diagonal = np.trace(np.fliplr(matrix)) / matrix.shape[0]

max_value = np.max(matrix)
min_value = np.min(matrix)

result_dict = {
    'sum': int(total_sum),
    'avr': total_avr,
    'sumMD': int(sum_main_diagonal),
    'avrMD': avr_main_diagonal,
    'sumSD': int(sum_secondary_diagonal),
    'avrSD': avr_secondary_diagonal,
    'max': int(max_value),
    'min': int(min_value)
}

with open('result.json', 'w') as json_file:
    json.dump(result_dict, json_file)

normalized_matrix = (matrix - np.min(matrix)) / (np.max(matrix) - np.min(matrix))
np.save('normalized_matrix.npy', normalized_matrix)
