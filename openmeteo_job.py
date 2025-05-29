from datetime import date
from openmeteo_api import OpenMeteoAPI
import pandas as pd
from load_to_db import load_file_to_db
from load_to_db import load_df_to_db
from data_utils import MIMU_Data
from Logger import Logger
from time import time
from time_utils import readable_time
from upload_to_s3 import upload_file_to_s3
import asyncio


def print_info(message: str):
    print(message)
    logger = Logger().get_logger("OpenMeteo Task")
    logger.info(message)


async def openmeteo_task():

    print_info("Getting townships from MIMU data")
    mimu = MIMU_Data()
    township_df = mimu.get_townships()
    # township_df = township_df.head(5)

    print_info("Extracting data from API")
    openmeteo_api = OpenMeteoAPI()

    # openmeteo - current
    openmeteo_current_df = await openmeteo_api.get_current(township_df)

    # str_today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'
    # file_path = f"./output/{str_today}_openmeteo_current.csv"
    file_path = f"./output/openmeteo_current.csv"

    if openmeteo_current_df is not None:
        openmeteo_current_df.to_csv(file_path, index=False, header=True)

    print_info("uploading csv file to s3")
    upload_file_to_s3(file_path)

    # print_info("load csv file to database")
    # load_file_to_db(file_path)

    # openmeteo - daily
    openmeteo_daily_df = await openmeteo_api.get_daily(township_df)
    # file_path = f"./output/{str_today}_openmeteo_forecast.csv"
    file_path = f"./output/openmeteo_forecast.csv"

    if openmeteo_daily_df is not None:
        openmeteo_daily_df.to_csv(file_path, index=False, header=True)

    print_info("uploading csv file to s3")
    upload_file_to_s3(file_path)

    # print_info("load csv file to database")
    # load_file_to_db(file_path)

    # print_info("load current_df and daily_df to database")
    # load_df_to_db(openmeteo_current_df, table_name="openmeteo_current")
    # load_df_to_db(openmeteo_daily_df, table_name="openmeteo_daily")


start_time = time()
asyncio.run(openmeteo_task())
end_time = time()

time_taken_seconds = end_time - start_time
hours, minutes, seconds = readable_time(time_taken_seconds)

print_info(f"Total time taken was {hours} hours, {minutes} minutes, {seconds} seconds.")
