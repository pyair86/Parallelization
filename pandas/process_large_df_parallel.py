import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import psutil
import time


# 1GB CSV with 8,000,000 rows x 22 columns
file_path = "example.csv"
data_frame = pd.read_csv(file_path)


def edit_df(df):

    df["EU_currency"] = df["Unit Price"].apply(
        lambda price: round(float(price) * 3.5, 3)
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


def edit_df_parallel(func, n_cores):
    start = time.time()

    df_split = np.array_split(data_frame, n_cores)

    with ProcessPoolExecutor(n_cores) as executor:
        future = executor.map(func, df_split)

    concat_df = pd.concat(future)

    end = time.time()

    print(f"processes time: {end - start}")
    # Multiprocessing time with 12 transformations: 209 seconds
    # Multiprocessing time with 7 transformations: 142 seconds

    print(concat_df)


def edit_df_no_parallelization():
    start = time.time()

    edited_df = edit_df(data_frame)

    end = time.time()

    print(f"no parallelization time: {end - start}")
    # no parallelization time with 12 transformations: 384 seconds
    # no parallelization time with 7 transformations: 215 seconds

    print(edited_df)


if __name__ == "__main__":

    physical_cpus = psutil.cpu_count(logical=False)

    edit_df_parallel(edit_df, physical_cpus)
    edit_df_no_parallelization()
