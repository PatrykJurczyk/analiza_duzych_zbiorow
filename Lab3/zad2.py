import dask.bag as db
import dask
from dask.distributed import Client
import json
from datetime import datetime
import os

def is_expired(record):
    expiry_date = record.get('credit-card', {}).get('expiration-date')
    month, year = map(int, expiry_date.split('/'))
    
    if year < 100:
        year += 2000

    now = datetime.now()
    return (year < now.year) or (year == now.year and month < now.month)

def main():
    client = Client()

    b = dask.datasets.make_people(npartitions=100, records_per_partition=10000)

    expired_bag = b.filter(is_expired)

    expired_count = expired_bag.count().compute()
    print(f"Liczba rekordów z wygasłymi kartami: {expired_count}")

    expired_bag = expired_bag.repartition(npartitions=10)

    if expired_count > 0:
        output_path = './data/expired_*.json'
        json_bag = expired_bag.map(json.dumps)
        json_bag.to_textfiles(output_path)
        
        print("Pliki zostały zapisane.")
    else:
        print("Brak rekordów z wygasłymi kartami do zapisania.")
    client.close()

if __name__ == '__main__':
    os.makedirs('./data', exist_ok=True)
    main()
