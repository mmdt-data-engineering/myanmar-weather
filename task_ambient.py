from datetime import date
from ambientweather_api import AmbientWeatherAPI
import pandas as pd
from load_to_db import load_file_to_db
from data_utils import MIMU_Data
from Logger import Logger
from time import time
from time_utils import readable_time
from upload_to_s3 import upload_file_to_s3


def print_info(message: str):
    print(message)
    logger = Logger().get_logger("ambient Task")
    logger.info(message)


def ambient_task():
    print_info("starting the task...")

    print_info("getting townships from MIMU data")
    mimu = MIMU_Data()
    township_df = mimu.get_townships()
    township_df = township_df.head(5)

    print_info("extracting data from api and save as csv file")
    ambient_api = AmbientWeatherAPI()

    # ambient - current
    ambient_forecast_df = ambient_api.get_forecast_df(township_df)

    str_today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'
    file_path = f"./output/{str_today}_ambient_forecast.csv"
    ambient_forecast_df.to_csv(file_path, index=False, header=True)

    print_info("uploading csv file to s3")
    upload_file_to_s3(file_path)

    print_info("load csv file to database")
    load_file_to_db(file_path)


start_time = time()
ambient_task()
end_time = time()

time_taken_seconds = end_time - start_time
hours, minutes, seconds = readable_time(time_taken_seconds)

print_info(f"Total time taken was {hours} hours, {minutes} minutes, {seconds} seconds.")
