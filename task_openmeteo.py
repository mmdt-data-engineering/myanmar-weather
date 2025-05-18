from datetime import date
from openmeteo_api import OpenMeteoAPI
import pandas as pd
from load_to_db import load_file_to_db
from data_utils import MIMU_Data
from Logger import Logger
from time import time
from time_utils import readable_time


def print_info(message: str):
    print(message)
    logger = Logger().get_logger("OpenMeteo Task")
    logger.info(message)


def openmeteo_task():
    print_info("starting the task...")

    print_info("getting townships from MIMU data")
    mimu = MIMU_Data()
    township_df = mimu.get_townships()
    township_df = township_df.head(50)

    print_info("extracting data from api and save as csv file")
    openmeteo_api = OpenMeteoAPI()

    # openmeteo - current
    openmeteo_current_df = openmeteo_api.get_current(township_df)

    str_today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'
    file_path = f"./output/{str_today}_openmeteo_current.csv"
    openmeteo_current_df.to_csv(file_path, index=False, header=True)

    print_info("load csv file to database")
    load_file_to_db(file_path)

    # openmeteo - daily
    openmeteo_daily_df = openmeteo_api.get_daily(township_df)
    file_path = f"./output/{str_today}_openmeteo_forecast.csv"
    openmeteo_daily_df.to_csv(file_path, index=False, header=True)

    print_info("load csv file to database")
    load_file_to_db(file_path)


start_time = time()
openmeteo_task()
end_time = time()

time_taken_seconds = end_time - start_time
hours, minutes, seconds = readable_time(time_taken_seconds)

print_info(f"Total time taken was {hours} hours, {minutes} minutes, {seconds} seconds.")
