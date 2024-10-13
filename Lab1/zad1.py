import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
import os
from multiprocessing import Pool, cpu_count
from io import StringIO

def load_data(file_names):
    dataframes = []
    for file in file_names:
        df = pd.read_parquet(file)
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

def analyze_dataframe(df):
    dtypes = df.dtypes
    memory_usage = df.memory_usage(deep=True).sum()
    return dtypes, memory_usage

def optimize_dtypes(df):
    for col in df.select_dtypes(include=['float']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.select_dtypes(include=['int']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype('category')
    return df

def plot_memory_comparison(original_memory, optimized_memory):
    labels = ['Oryginalne', 'Zoptymalizowane']
    memory_usage = [original_memory, optimized_memory]
    
    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots()
    bars = ax.bar(x, memory_usage, width, label='Zużycie pamięci (MB)', color=['#1f77b4', '#ff7f0e'])
    
    ax.set_ylabel('Zużycie pamięci (MB)')
    ax.set_title('Porównanie zużycia pamięci RAM przed i po optymalizacji')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.bar_label(bars)
    plt.tight_layout()
    plt.show()

def measure_execution_time(func, *args):
    start_time = time.time()
    result = func(*args)
    end_time = time.time()
    return result, end_time - start_time

def perform_operations(df):
    grouped = df.groupby('profile_id').agg({
        'likes': 'mean',
        'comments': 'mean'
    }).reset_index()

    filtered = df[df['followers'] > 1000]
    sorted_df = df.sort_values(by='likes', ascending=True)

    return grouped, filtered, sorted_df

def save_as_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f"DataFrame zapisano jako plik CSV: {filename}")

def calculate_parquet_size(file_names):
    total_size = sum(os.path.getsize(file) for file in file_names)
    return total_size / (1024 ** 2)  # rozmiar w MB

def load_csv_entirely(filename):
    df = pd.read_csv(filename, encoding='utf-8')
    return df

def load_csv_chunks(filename, chunksize):
    df_chunks = pd.read_csv(filename, chunksize=chunksize, encoding='utf-8')
    df = pd.concat(df_chunks, ignore_index=True)
    return df

def load_chunk(chunk):
    return pd.read_csv(StringIO(''.join(chunk)), encoding='utf-8')

def load_csv_multiprocessing(filename, n_processes):
    chunk_size = os.path.getsize(filename) // n_processes
    chunks = []

    with open(filename, 'r', encoding='utf-8') as f:
        header = f.readline()
        while True:
            chunk = f.readlines(chunk_size)
            if not chunk:
                break
            chunks.append(''.join(chunk))

    with Pool(processes=n_processes) as pool:
        df_list = pool.map(load_chunk, chunks)
    
    return pd.concat(df_list, ignore_index=True)

def main():
    file_names = [f"./Data/{str(i).zfill(4)}.parquet" for i in range(1, 2)]

    full_df = load_data(file_names)
    original_dtypes, original_memory_usage = analyze_dataframe(full_df)

    with open('analysis_results.txt', 'w') as f:
        f.write("Typy danych dla każdej kolumny:\n")
        f.write(str(original_dtypes) + "\n\n")
        f.write(f"Całkowite zużycie pamięci RAM (domyślne typy danych): {original_memory_usage / 1024 ** 2:.2f} MB\n")

    optimized_df = optimize_dtypes(full_df)
    optimized_memory_usage = optimized_df.memory_usage(deep=True).sum()

    print("Typy danych przed optymalizacją:")
    print(original_dtypes)
    print(f"\nCałkowite zużycie pamięci RAM przed optymalizacją: {original_memory_usage / 1024 ** 2:.2f} MB")
    
    print("\nTypy danych po optymalizacji:")
    optimized_dtypes = optimized_df.dtypes
    print(optimized_dtypes)
    print(f"\nCałkowite zużycie pamięci RAM po optymalizacji: {optimized_memory_usage / 1024 ** 2:.2f} MB")

    plot_memory_comparison(original_memory_usage / 1024 ** 2, optimized_memory_usage / 1024 ** 2)

    print("\nWykonywanie operacji na oryginalnych danych...")
    original_results, original_time = measure_execution_time(perform_operations, full_df)
    
    print("Wykonywanie operacji na zoptymalizowanych danych...")
    optimized_results, optimized_time = measure_execution_time(perform_operations, optimized_df)

    print(f"Czas wykonania operacji na oryginalnych danych: {original_time:.4f} sekund")
    print(f"Czas wykonania operacji na zoptymalizowanych danych: {optimized_time:.4f} sekund")

    csv_filename = "data_export.csv"
    save_as_csv(optimized_df, csv_filename)

    parquet_size = calculate_parquet_size(file_names)
    csv_size = os.path.getsize(csv_filename) / (1024 ** 2)
    print(f"Rozmiar plików Parquet: {parquet_size:.2f} MB")
    print(f"Rozmiar pliku CSV: {csv_size:.2f} MB")
    print(f"Różnica w rozmiarze: {parquet_size - csv_size:.2f} MB")
    print("\nMierzenie czasu wczytywania pliku CSV...")

    df_entire, csv_time_entire = measure_execution_time(load_csv_entirely, csv_filename)
    print(f"Czas wczytywania całego pliku CSV: {csv_time_entire:.4f} sekund")

    chunksize = 10000
    df_chunks, csv_time_chunks = measure_execution_time(load_csv_chunks, csv_filename, chunksize)
    print(f"Czas wczytywania pliku CSV z chunksize={chunksize}: {csv_time_chunks:.4f} sekund")

    n_processes1 = cpu_count() - 2
    df_multiprocessing1, csv_time_multiprocessing1 = measure_execution_time(load_csv_multiprocessing, csv_filename, n_processes1)
    print(f"Czas wczytywania pliku CSV z multiprocesowaniem (rdzenie - 2): {csv_time_multiprocessing1:.4f} sekund")

    n_processes2 = (cpu_count() - 2) * 2
    df_multiprocessing2, csv_time_multiprocessing2 = measure_execution_time(load_csv_multiprocessing, csv_filename, n_processes2)
    print(f"Czas wczytywania pliku CSV z multiprocesowaniem (rdzenie - 2 * 2): {csv_time_multiprocessing2:.4f} sekund")

if __name__ == "__main__":
    main()
