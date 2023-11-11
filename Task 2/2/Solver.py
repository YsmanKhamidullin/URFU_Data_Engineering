import numpy as np
import os

matrix = np.load('matrix_96_2.npy')
threshold_value = 596

indices = np.where(matrix > threshold_value)
x = indices[0]
y = indices[1]
z = matrix[indices]

np.savez('result_arrays.npz', x=x, y=y, z=z)
np.savez_compressed('result_arrays_compressed.npz', x=x, y=y, z=z)

file_size = os.path.getsize('result_arrays.npz')
compressed_file_size = os.path.getsize('result_arrays_compressed.npz')

print(f"Размер файла: {file_size} байт")
print(f"Размер сжатого файла: {compressed_file_size} байт")
