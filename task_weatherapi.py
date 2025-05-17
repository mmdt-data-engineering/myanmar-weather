from datetime import date
from weather_api import WeatherAPI
import pandas as pd
from load_to_db import load_file_to_db
from data_utils import MIMU_Data
from Logger import Logger


def print_info(message: str):
    print(message)
    logger = Logger().get_logger("WeatherAPI Task")
    logger.info(message)


def weatherapi_task():
    print_info("starting the task...")

    print_info("getting townships from MIMU data")
    mimu = MIMU_Data()
    township_df = mimu.get_townships()
    township_df = township_df.head(5)

    print_info("extracting data from api and save as csv file")
    weather_api = WeatherAPI()

    # weatherapi - current
    weatherapi_current_df = weather_api.get_current(township_df)

    str_today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'
    file_path = f"./output/{str_today}_weatherapi_current.csv"
    weatherapi_current_df.to_csv(file_path, index=False, header=True)

    print_info("load csv file to database")
    load_file_to_db(file_path)

    # weatherapi - daily
    weatherapi_daily_df = weather_api.get_daily(township_df, no_of_days=7)
    file_path = f"./output/{str_today}_weatherapi_forecast.csv"
    weatherapi_daily_df.to_csv(file_path, index=False, header=True)

    print_info("load csv file to database")
    load_file_to_db(file_path)


weatherapi_task()
