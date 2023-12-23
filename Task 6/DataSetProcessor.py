import os
import random
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


class DataSetProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.dataset = None

    def load_dataset(self):
        file_size = os.path.getsize(self.file_path)
        print(f"File size: {file_size / 1024 ** 2} MB")
        # self.dataset = pd.read_csv(self.file_path, engine='python', index_col=False)
        chunk_size = 150000
        chunks = pd.read_csv(self.file_path, chunksize=chunk_size, low_memory=False, index_col=False)

        for chunk in chunks:
            self.dataset = pd.concat([self.dataset, chunk], ignore_index=True)
            print(self.dataset.columns)
            return

    def analyze_memory(self):
        memory_usage_in_memory = self.dataset.memory_usage(deep=True).sum()
        print("Объем памяти: {:.2f} MB".format(memory_usage_in_memory / (1024 ** 2)))
        column_memory_info = pd.DataFrame({
            'memory_usage': self.dataset.memory_usage(deep=True),
            'percentage_of_total': self.dataset.memory_usage(deep=True) / memory_usage_in_memory * 100,
            'dtype': self.dataset.dtypes
        })

        sorted_df = column_memory_info.sort_values(by='memory_usage', ascending=False)
        return sorted_df

    def sort_and_save_stats(self, sorted_df, json_output_file):
        sorted_df.to_json(json_output_file, orient='table', default_handler=str, indent=2)
        print("Данные сохранены в файл:", json_output_file)

    def optimize_dataset(self):
        for column in self.dataset.columns:
            if self.dataset[column].dtype == "object":
                unique_values_ratio = self.dataset[column].nunique() / len(self.dataset[column])
                if unique_values_ratio < 0.5:
                    self.dataset[column] = self.dataset[column].astype("category")

        int_columns = self.dataset.select_dtypes(include=["int"]).columns
        self.dataset[int_columns] = self.dataset[int_columns].apply(pd.to_numeric, downcast="integer")

        float_columns = self.dataset.select_dtypes(include=["float"]).columns
        self.dataset[float_columns] = self.dataset[float_columns].apply(pd.to_numeric, downcast="float")

    def get_first_10_numeric_columns(self):
        selected_columns = []
        for col in self.dataset.columns:
            if pd.api.types.is_numeric_dtype(self.dataset[col]):
                selected_columns.append(col)
                if len(selected_columns) == 10:
                    break
        return selected_columns

    def select_and_save_subset(self, output_file):
        selected_columns = self.get_first_10_numeric_columns()
        if len(selected_columns) < 10:
            raise ValueError("Not enough numeric columns available.")

        chunk_size = 80000

        chunks = pd.read_csv(self.file_path, usecols=selected_columns, chunksize=chunk_size)
        subset = pd.DataFrame()

        for chunk in chunks:
            subset = pd.concat([subset, chunk], ignore_index=True)

        subset.to_csv(output_file, index=False)
        print("Subset saved to:", output_file)

    def additional_optimize(self, numeric_columns, numeric_threshold):
        i = 0
        for column in numeric_columns:
            self.dataset = self.dataset[self.dataset[column] >= numeric_threshold[i]]
            i += 1
