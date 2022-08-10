"""

Imagine we need to convert a LARGE AMOUNT of large files, CSV - Parquet for example.

With 5 files, speed was improved from 131 to 91 seconds with parallelization.
With 10 files, speed was improved from 314 to 199 seconds with parallelization.

Increasing the number of files will result more optimization.

"""

from concurrent.futures import ProcessPoolExecutor
import time
import pandas as pd

# 1GB CSVs with 8,000,000 rows x 22 columns
files_paths = [
    "example.csv",
    "example2.csv",
    "example3.csv",
    "example4.csv",
    "example5.csv",
    "example6.csv",
    "example7.csv",
    "example8.csv",
    "example9.csv",
    "example10.csv",
]


def convert_csv_to_parquet(file_path):

    df = pd.read_csv(file_path)
    parquet_extension = ".parquet"
    df.to_parquet(file_path.split(".")[0] + parquet_extension)


def convert_csv_to_parquet_processes():

    with ProcessPoolExecutor() as executor:
        executor.map(convert_csv_to_parquet, files_paths)


def convert_csv_to_parquet_no_parallelization():

    for path in files_paths:
        convert_csv_to_parquet(path)


def measure_time(function_to_measure, is_multiprocessing):
    
    start = time.time()
    function_to_measure()
    end = time.time()

    print_measurement(start, end, is_multiprocessing)
    
    
def print_measurement(start, end, is_multiprocessing):

    if is_multiprocessing:
        print(f"processes time: {end - start}")
    else:
        print(f"no parallelization time: {end - start}")


if __name__ == "__main__":
    
    measure_time(convert_csv_to_parquet_processes, True)
    measure_time(convert_csv_to_parquet_no_parallelization, False)
