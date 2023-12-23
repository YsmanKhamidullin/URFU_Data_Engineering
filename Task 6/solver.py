from DataSetProcessor import DataSetProcessor as ds
import plots_builder

import os


def algo(file_path):
    processor = ds(file_path)
    processor.load_dataset()
    print("Dataset Loaded")
    sorted_df = processor.analyze_memory()
    print("analyze_memory finish")
    processor.sort_and_save_stats(sorted_df, output_file_stats)
    print("sort_and_save_stats finish")
    processor.optimize_dataset()
    print("optimize_dataset finish")
    sorted_df = processor.analyze_memory()
    processor.sort_and_save_stats(sorted_df, output_file_optimized_stats)
    print("analyze_memory_after_optimization finish")
    processor.select_and_save_subset(output_file_subset)
    print("select_and_save_subset finish")


file_paths = [
    "DataSet/[1]game_logs/dataset.csv",
    "DataSet/[2]automotive/dataset.csv",
    "DataSet/[3]flights/dataset.csv",
    "DataSet/[4]vacancies/dataset.csv",
    "DataSet/[5]asteroid/dataset.csv",
    "DataSet/[6]physical/dataset.csv",
]

# for i, file_path in enumerate(file_paths, start=1):
#     if i > 5:
#         print(f"Processing file {i}: {file_path}")
#         output_file_stats = f"out/{i}/stats.json"
#         output_file_optimized_stats = f"out/{i}/stats_optimized.json"
#         output_file_subset = f"out/{i}/subset.csv"
#         output_plots_folder = f"out/{i}/plots"
#
#         os.makedirs(os.path.dirname(output_file_stats), exist_ok=True)
#         os.makedirs(os.path.dirname(output_file_optimized_stats), exist_ok=True)
#         os.makedirs(os.path.dirname(output_file_subset), exist_ok=True)
#         os.makedirs(output_plots_folder, exist_ok=True)
#
#         algo(file_path)
#         print(f"Processed file {i}: {file_path}")

output_file_subset = f"out/6/subset.csv"
output_plots_folder = f"out/6/plots"
categor_var = 'heart_rate'
categor_column = 'hand temperature (°C)'
num_columns = ['hand gyroscope X',
               'hand gyroscope Y']

plots_builder.create_plots(output_file_subset, output_plots_folder, categor_var, categor_column,
                           num_columns)
