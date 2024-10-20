import dask.bag as db
import dask
from dask.distributed import Client
import os

def is_adult(record):
    return record.get('age', 0) >= 18

def main():
    client = Client()

    b = dask.datasets.make_people(npartitions=100, records_per_partition=10000)
    adults_bag = b.filter(is_adult)

    adults_df = adults_bag.to_dataframe(meta={'name': str, 'age': int, 'occupation': str})

    print("Struktura danych dorosłych:")
    print(adults_df.head())

    output_path = './data/adults.parquet'
    adults_df.repartition(npartitions=1).to_parquet(output_path)

    print(f"Dane dorosłych zostały zapisane do pliku: {output_path}")
    client.close()

if __name__ == '__main__':
    os.makedirs('./data', exist_ok=True)
    main()
