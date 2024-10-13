import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask.distributed import Client
import time

def zadanie_1():
    df = pd.read_csv("zamowienia.csv", sep=';')
    print("Pierwsze wiersze danych:")
    print(df.head())
    np.random.seed(0)
    num_rows = df.shape[0]
    indices = np.random.choice(range(10, num_rows), size=5, replace=False) if num_rows > 10 else []
    for index in indices:
        df.iloc[index, 1] = np.nan
    df.to_csv("zamowienia_missing.csv", sep=';', index=False)
    print("Zapisano plik zamowienia_missing.csv.")
    ddf = dd.read_csv("zamowienia_missing.csv", sep=';')
    print("Typy danych w Dask DataFrame:")
    print(ddf.dtypes)
    try:
        ddf.compute()
        print("Obliczenia wykonane pomyślnie.")
    except Exception as e:
        print("Wystąpił błąd podczas wykonywania obliczeń:", e)

def zadanie_2():
    client = Client(n_workers=4, dashboard_address=':8787')
    print("Lokalny klaster Dask uruchomiony z 4 workerami.")
    print("Dashboard dostępny pod adresem:", client.dashboard_link)
    input("Naciśnij Enter, aby zakończyć klaster Dask...")
    client.close()

def zadanie_3():
    client = Client(n_workers=4, memory_limit='8GB', dashboard_address=':8789')
    print("Klient Dask skonfigurowany.")
    file_names = [f"./Data/{str(i).zfill(4)}.parquet" for i in range(1, 3)]
    start_time = time.time()
    try:
        ddf = dd.read_parquet(file_names)
        print("Czas ładowania danych:", time.time() - start_time, "sekundy")
    except Exception as e:
        print("Wystąpił błąd podczas ładowania danych:", e)
        return None
    return ddf

def zadanie_4(ddf):
    if ddf is None:
        print("Brak danych do przetworzenia.")
        return
    print("Dostępne kolumny w Dask DataFrame:", ddf.columns.tolist())
    start_time = time.time()
    try:
        if 'username' in ddf.columns and 'likes' in ddf.columns:
            top_users = ddf[['username', 'likes']].nlargest(10, 'likes').compute()
            print("Top 10 użytkowników z największą liczbą like'ów:")
            print(top_users)
        else:
            print("Kolumny 'username' i 'likes' nie istnieją w danych.")
    except Exception as e:
        print("Wystąpił błąd podczas przetwarzania danych:", e)
    start_time = time.time()
    try:
        if 'date' in ddf.columns:
            first_half_2019 = ddf[ddf['date'] < '2019-07-01']
            print("Czas operacji (pierwsze półrocze 2019):", time.time() - start_time, "sekundy")
            print("Dane za pierwsze półrocze 2019 roku:")
            print(first_half_2019.compute())
        else:
            print("Kolumna 'date' nie istnieje w danych.")
    except Exception as e:
        print("Wystąpił błąd podczas filtrowania danych:", e)

def zadanie_5():
    client = Client(n_workers=4, memory_limit='8GB', dashboard_address=':8788')
    print("Klient Dask skonfigurowany.")
    file_names = [f"./Data/{str(i).zfill(4)}.parquet" for i in range(1, 3)]
    start_time = time.time()
    try:
        ddf = dd.read_parquet(file_names, 
                               dtype={'username': 'str', 'likes': 'int64', 'date': 'datetime64[ns]'})
        print("Czas ładowania danych:", time.time() - start_time, "sekundy")
    except Exception as e:
        print("Wystąpił błąd podczas ładowania danych:", e)
        return None
    start_time = time.time()
    try:
        if 'username' in ddf.columns and 'likes' in ddf.columns:
            top_users = ddf[['username', 'likes']].nlargest(10, 'likes').compute()
            print("Top 10 użytkowników z największą liczbą like'ów po optymalizacji:")
            print(top_users)
        else:
            print("Kolumny 'username' i 'likes' nie istnieją w danych.")
    except Exception as e:
        print("Wystąpił błąd podczas przetwarzania danych:", e)

if __name__ == "__main__":
    zadanie_1()
    # zadanie_2()
    # ddf = zadanie_3()
    # zadanie_4(ddf)
    # zadanie_5()
