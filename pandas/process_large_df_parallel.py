"""

Imagine we need to transorm a LARGE file, 1GB CSV with 8,000,000 rows x 22 columns for example.

With 7 transformations, speed was improved from 215 to 142 seconds with parallelization.
With 12 transformations, speed was improved from 384 to 209 seconds with parallelization - almost half of it!

Increasing the number of transformations and rows will result more optimization.

"""

import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import psutil
import time


# 1GB CSV with 8,000,000 rows x 22 columns
file_path = "example.csv"
data_frame = pd.read_csv(file_path)


def edit_df(df):

    eu_currency_conversion = 3.5

    df["EU_currency"] = df["Unit Price"].apply(
        lambda price: round(float(price) * eu_currency_conversion, 2)
    )
    df[["Ship Date", "Order Date"]] = df[["Ship Date", "Order Date"]].apply(
        lambda x: pd.to_datetime(x, errors="coerce", format="%m/%d/%Y")
    )

    df["day_order_date_delta"] = df["Order Date"].diff()
    df["day_order_date_delta"] = (
        df["Order Date"]
        .diff()
        .astype(str)
        .apply(lambda x: x.split()[0])
        .apply(lambda x: 0 if x == "NaT" else int(x))
    )

    df["diff_days_order_date_ship_date"] = df["Ship Date"] - df["Order Date"]

    df["Region"] = df["Region"].apply(lambda x: x.capitalize())
    df["Country"] = df["Country"].apply(lambda x: x.capitalize())

    return df


def edit_df_parallel():

    physical_cpus = psutil.cpu_count(logical=False)

    df_split = np.array_split(data_frame, physical_cpus)

    with ProcessPoolExecutor(physical_cpus) as executor:
        future = executor.map(edit_df, df_split)

    concat_df = pd.concat(future)


def edit_df_no_parallelization():

    edited_df = edit_df(data_frame)


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

    measure_time(edit_df_parallel, True)
    measure_time(edit_df_no_parallelization, False)
