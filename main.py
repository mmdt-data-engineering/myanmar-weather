from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI
from ambientweather_api import AmbientWeatherAPI
from datetime import date
from meteoblue_api import MeteoBlueWeatherAPI
import asyncio
import pandas as pd
from time import time
from time_utils import readable_time


async def fetch_ambient_data(township_df: pd.DataFrame):
    today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'

    # Fetch AmbientWeather data
    ambient_api = AmbientWeatherAPI()  # New instance for AmbientWeatherAPI

    ambient_df = await ambient_api.get_forecast_df(township_df)  # Get forecast data

    if ambient_df is None or ambient_df.empty:
        raise ValueError("Ambient dataframe is empty")
    else:
        filename = f"./output/{today}_ambientweather_data_forecast.csv"
        ambient_df.to_csv(filename, index=False, header=True)


async def fetch_meteoblue_data(township_df: pd.DataFrame):

    today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'

    # Fetch MeteoBlue data
    meteoblue_api = MeteoBlueWeatherAPI()
    meteoblue_df = await meteoblue_api.get_meteoblue_current_weather_data(township_df)

    if meteoblue_df is None or meteoblue_df.empty: 
        raise ValueError("meteo-blue current dataframe is empty")
    else: 
        filename = f"./output/{today}_meteoblue_current.csv"
        meteoblue_df.to_csv(filename, index=False, header=True)

    meteoblue_df = await meteoblue_api.get_meteoblue_forecast_weather_data(township_df)

    if meteoblue_df is None or meteoblue_df.empty:
        raise ValueError("meteo-blue daily dataframe is empty")
    else: 
        filename = f"./output/{today}_meteoblue_forecast.csv"
        meteoblue_df.to_csv(filename, index=False, header=True)


async def fetch_openmeteo_data(township_df: pd.DataFrame):

    today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'

    openmeteo_api = OpenMeteoAPI()

    # open-meteo - current
    openmeteo_current_df = await openmeteo_api.get_current(township_df)

    if openmeteo_current_df is None or openmeteo_current_df.empty:
        raise ValueError("open-meteo current dataframe is empty")
    else:
        filename = f"./output/{today}_open_meteo_current.csv"
        openmeteo_current_df.to_csv(filename, index=False, header=True)

    # open-meteo - daily
    openmeteo_daily_df = await openmeteo_api.get_daily(township_df)

    if openmeteo_daily_df is None or openmeteo_daily_df.empty:
        raise ValueError("open-meteo daily dataframe is empty")
    else:
        filename = f"./output/{today}_open_meteo_forecast.csv"
        openmeteo_daily_df.to_csv(filename, index=False, header=True)


async def fetch_weatherapi_data(township_df:pd.DataFrame):

    today = date.today().strftime("%Y-%m-%d")  # Output like '2025-05-16'

    # weatherapi - current
    weather_api = WeatherAPI()
    weatherapi_current_df = await weather_api.get_current(township_df)

    if weatherapi_current_df is None or weatherapi_current_df.empty:
        raise ValueError("WeatherAPI current daraframe is empty")
    else:
        filename = f"./output/{today}_weatherapi_current.csv"
        weatherapi_current_df.to_csv(filename, index=False, header=True)

    # weatherapi - daily
    weatherapi_daily_df = await weather_api.get_daily(township_df, no_of_days=7)
    if weatherapi_daily_df is None or weatherapi_daily_df.empty: 
        raise ValueError("WeatherAPI daily dataframe is empty")
    else:
        filename = f"./output/{today}_weatherapi_forecast.csv"
        weatherapi_daily_df.to_csv(filename, index=False, header=True)

if __name__ == "__main__":

    start_time = time()

    # Get townships from MIMU data
    mimu_data = MIMU_Data()
    township_df = mimu_data.get_townships()
    township_df = township_df.head(50)

    asyncio.run(fetch_ambient_data(township_df))
    
    asyncio.run(fetch_meteoblue_data(township_df))
    
    asyncio.run(fetch_openmeteo_data(township_df))
    
    asyncio.run(fetch_weatherapi_data(township_df))

    end_time = time()
    time_taken_seconds = end_time - start_time
    hours, minutes, seconds = readable_time(time_taken_seconds)
    print(f"Total time taken was {hours} hours, {minutes} minutes, {seconds} seconds.")



