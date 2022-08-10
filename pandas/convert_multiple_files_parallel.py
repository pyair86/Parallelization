"""

Imagine we need to convert a LARGE AMOUNT of large files, CSV - Parquet for example.

With only 5 files, speed was improved from 131 to 91 seconds with parallelization.
With only 10 files, speed was improved from 314 to 199 seconds with parallelization - almost 4 minutes!

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


def convert_csv_to_par(file_path):
    df = pd.read_csv(file_path)
    parquet_extension = ".parquet"
    df.to_parquet(file_path.split(".")[0] + parquet_extension)


def convert_csv_to_par_processes():
    start = time.time()

    with ProcessPoolExecutor() as executor:
        executor.map(convert_csv_to_par, files_paths)

    end = time.time()

    print(f"processes time: {end - start}")
    # with 5 files: 91 seconds
    # with 10 files: 199 seconds


def convert_csv_to_par_no_parallelization():
    start = time.time()

    for path in files_paths:
        convert_csv_to_par(path)

    end = time.time()

    print(f"no parallelization time: {end - start}")
    # with 5 files: 131 seconds
    # with 10 files: 314 seconds


if __name__ == "__main__":
    convert_csv_to_par_processes()
    convert_csv_to_par_no_parallelization()
