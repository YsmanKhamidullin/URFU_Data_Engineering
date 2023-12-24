import os
import random
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from DataSetProcessor import DataSetProcessor


def create_plots(path_to_csv, path_folder, categor_var, categor_column,
                 num_columns):
    optimized_processor = DataSetProcessor(path_to_csv)
    optimized_processor.load_dataset()
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)

    # plot_bar_chart(categor_column, optimized_processor, path_folder)
    # plot_pie_chart(categor_var, optimized_processor, path_folder)
    # plot_scatter_plot(num_columns, optimized_processor, path_folder)
    # plot_line_chart(num_columns, optimized_processor, path_folder)
    plot_histogram(categor_var, optimized_processor, path_folder)


def plot_histogram(categor_column, optimized_processor, path_folder):
    try:
        plt.figure(figsize=(15, 6))
        optimized_processor.dataset.plot.hist(column=categor_column, bins=40, density=True)
        plt.title(f'Histogram: Frequency of Unique Values: {categor_column}')
        save_plot(plt, path_folder, 'histogram.png')
    except Exception as e:
        print(f"An error occurred in plot_bar_chart: {e}")


def save_plot(plot, path_folder, file_name):
    try:
        plot.savefig(os.path.join(path_folder, file_name))
        plot.close()
        print(f"Plot saved successfully: {file_name}")
    except Exception as e:
        print(f"Error saving plot {file_name}: {e}")


def plot_bar_chart(selected_categorical_column, optimized_processor, path_folder):
    try:
        plt.figure(figsize=(15, 6))

        value_counts = optimized_processor.dataset[selected_categorical_column].value_counts()
        threshold = value_counts.max() / 6.0
        filtered_counts = value_counts[value_counts >= threshold]
        filtered_counts.plot.bar()
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
        plt.figure(figsize=(6, 6))
        value_counts = optimized_processor.dataset[selected_categorical_variable].value_counts()
        threshold = value_counts.max() / 2.0
        filtered_counts = value_counts[value_counts >= threshold]

        filtered_counts.plot.pie(autopct='%1.1f%%', startangle=90)
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
