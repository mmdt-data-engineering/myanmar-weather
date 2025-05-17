from datetime import date
from meteoblue_api import MeteoBlueWeatherAPI
import pandas as pd
from load_to_db import load_file_to_db
from data_utils import MIMU_Data
from Logger import Logger


def print_info(message: str):
    print(message)
    logger = Logger().get_logger("meteoblue Task")
    logger.info(message)


def meteoblue_task():
    print_info("starting the task...")

    print_info("getting townships from MIMU data")
    mimu = MIMU_Data()
    township_df = mimu.get_townships()
    township_df = township_df.head(5)

    print_info("extracting data from api and save as csv file")
    meteoblue_api = MeteoBlueWeatherAPI()

    str_today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'

    # meteoblue - current
    meteoblue_df = meteoblue_api.get_meteoblue_current_weather_data(township_df)

    filename = f"./output/{str_today}_meteoblue_current.csv"
    meteoblue_df.to_csv(filename, index=False, header=True)

    print_info("load csv file to database")
    load_file_to_db(file_path)

    # meteoblue - forecast
    meteoblue_df = meteoblue_api.get_meteoblue_forecast_weather_data(township_df)

    file_path = f"./output/{str_today}_meteoblue_forecast.csv"
    meteoblue_df.to_csv(file_path, index=False, header=True)

    print_info("load csv file to database")
    load_file_to_db(file_path)


meteoblue_task()
