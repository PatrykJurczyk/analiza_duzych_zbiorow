from datetime import datetime
import dask
import pandas as pd
import time
import os

def load_log_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    return lines

def parse(inp: str):
    record = {}
    
    date_start = inp.find('[') + 1
    date_end = inp.find(']')
    date_s = slice(date_start, date_end)

    level_start = inp.find('[', date_end) + 1
    level_end = inp.find(']', level_start)
    level_s = slice(level_start, level_end)

    client_start = inp.find('[', level_end)
    client_end = inp.find(']', client_start)

    record["date"] = inp[date_s]    
    record["level"] = inp[level_s]
    record["client"] = "" if client_start == -1 else inp[client_start + 8: client_end]
    record["message"] = inp[client_end + 2:] if record["client"] else inp[level_end + 2:]
    
    return record

def convert_date(rec):
    rec["date"] = datetime.strptime(rec["date"], "%a %b %d %H:%M:%S %Y")
    return rec

def sequential_processing(lines):
    output = []
    for line in lines:
        record = parse(line)
        record = convert_date(record)
        output.append(list(record.values()))
    return output

@dask.delayed
def delayed_parse(line):
    return parse(line)

@dask.delayed
def delayed_convert_date(record):
    return convert_date(record)

def parallel_processing(lines):
    delayed_records = [delayed_parse(line) for line in lines]
    delayed_converted = [delayed_convert_date(record) for record in delayed_records]
    output = dask.compute(*delayed_converted)
    return output

logfile_path = './Apache_10k.log'

lines = load_log_file(logfile_path)

start_time = time.time()
sequential_output = sequential_processing(lines)
sequential_duration = time.time() - start_time
print(f"Sequential processing took {sequential_duration:.4f} seconds")

start_time = time.time()
parallel_output = parallel_processing(lines)
parallel_duration = time.time() - start_time
print(f"Parallel processing took {parallel_duration:.4f} seconds")

df = pd.DataFrame(parallel_output, columns=["date", "level", "client", "message"])

output_dir = "./output_logs"
os.makedirs(output_dir, exist_ok=True)
df.to_parquet(os.path.join(output_dir, "logs.parquet"))

print(df.head())
