import os
import random
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from DataSetProcessor import DataSetProcessor


def create_plots(path_to_csv, path_folder):
    optimized_processor = DataSetProcessor(path_to_csv)
    optimized_processor.load_dataset()

    if not os.path.exists(path_folder):
        os.makedirs(path_folder)

    list_dataset = optimized_processor.dataset.columns.tolist()
    selected_columns = [col for col in list_dataset if pd.api.types.is_numeric_dtype(optimized_processor.dataset[col])]

    selected_categorical_variable = random.choice(selected_columns)
    selected_columns.remove(selected_categorical_variable)

    selected_categorical_column = random.choice(selected_columns)
    selected_columns.remove(selected_categorical_column)

    selected_numeric_columns = random.sample(selected_columns, 2)

    plot_bar_chart(selected_categorical_column, optimized_processor, path_folder)
    plot_line_chart(selected_numeric_columns, optimized_processor, path_folder)
    plot_pie_chart(selected_categorical_variable, optimized_processor, path_folder)
    plot_scatter_plot(selected_numeric_columns, optimized_processor, path_folder)


def save_plot(plot, path_folder, file_name):
    try:
        plot.savefig(os.path.join(path_folder, file_name))
        plot.close()
        print(f"Plot saved successfully: {file_name}")
    except Exception as e:
        print(f"Error saving plot {file_name}: {e}")


def plot_bar_chart(selected_categorical_column, optimized_processor, path_folder):
    try:
        plt.figure(figsize=(10, 6))
        sns.countplot(x=selected_categorical_column, data=optimized_processor.dataset)
        plt.title(f'Bar Chart: Count of Unique Values in Categorical Column. x:{selected_categorical_column}')
        save_plot(plt, path_folder, 'bar_chart.png')
    except Exception as e:
        print(f"An error occurred in plot_bar_chart: {e}")


def plot_line_chart(selected_numeric_columns, optimized_processor, path_folder):
    try:
        plt.figure(figsize=(10, 6))
        sns.lineplot(x=selected_numeric_columns[0], y=selected_numeric_columns[1], data=optimized_processor.dataset)
        plt.title(
            f'Line Plot: Relationship between Numeric Columns. x: {selected_numeric_columns[0]} y: {selected_numeric_columns[1]}')
        save_plot(plt, path_folder, 'line_plot.png')
    except Exception as e:
        print(f"An error occurred in plot_line_chart: {e}")


def plot_pie_chart(selected_categorical_variable, optimized_processor, path_folder):
    try:
        plt.figure(figsize=(12, 12))
        optimized_processor.dataset[selected_categorical_variable].value_counts().plot.pie()
        plt.title(f'Pie Chart: Distribution of Categorical Variable: {selected_categorical_variable}')
        save_plot(plt, path_folder, 'pie_chart.png')
    except Exception as e:
        print(f"An error occurred in plot_pie_chart: {e}")


def plot_scatter_plot(selected_numeric_columns, optimized_processor, path_folder):
    try:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x=selected_numeric_columns[0], y=selected_numeric_columns[1], data=optimized_processor.dataset)
        plt.title(
            f'Scatter Plot: Relationship between Numeric Columns. x: {selected_numeric_columns[0]} y: {selected_numeric_columns[1]}')
        save_plot(plt, path_folder, 'scatter_plot.png')
    except Exception as e:
        print(f"An error occurred in plot_scatter_plot: {e}")
