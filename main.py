from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI
from ambientweather_api import AmbientWeatherAPI
from datetime import date
from meteoblue_api import MeteoBlueWeatherAPI
import asyncio


async def fetch_weather_data():
    # Get townships from MIMU data
    mimu_data = MIMU_Data()
    township_df = mimu_data.get_townships()

    township_df = township_df.head(50)

    weather_api = WeatherAPI()

    today = date.today()
    str_today = today.strftime("%Y-%m-%d")  # Output like '2025-05-16'

    # weatherapi - current
    weatherapi_current_df = await weather_api.get_current(township_df)
    filename = f"./output/{str_today}_weatherapi_current.csv"
    weatherapi_current_df.to_csv(filename, index=False, header=True)

    # weatherapi - daily
    weatherapi_daily_df = await weather_api.get_daily(township_df, no_of_days=7)
    filename = f"./output/{str_today}_weatherapi_forecast.csv"
    weatherapi_daily_df.to_csv(filename, index=False, header=True)

    openmeteo_api = OpenMeteoAPI()

    # open-meteo - current
    openmeteo_current_df = await openmeteo_api.get_current(township_df)
    filename = f"./output/{str_today}_open_meteo_current.csv"
    openmeteo_current_df.to_csv(filename, index=False, header=True)

    # open-meteo - daily
    openmeteo_daily_df = await openmeteo_api.get_daily(township_df)
    filename = f"./output/{str_today}_open_meteo_forecast.csv"
    openmeteo_daily_df.to_csv(filename, index=False, header=True)

    # Fetch AmbientWeather data
    ambient_api = AmbientWeatherAPI()  # New instance for AmbientWeatherAPI

    ambient_df = await ambient_api.get_forecast_df(township_df)  # Get forecast data
    filename = f"./output/{str_today}_ambientweather_data_forecast.csv"
    ambient_df.to_csv(filename, index=False, header=True)

    # Fetch MeteoBlue data
    meteoblue_api = MeteoBlueWeatherAPI()
    meteoblue_df = meteoblue_api.get_meteoblue_current_weather_data(township_df)
    filename = f"./output/{str_today}_meteoblue_current.csv"
    meteoblue_df.to_csv(filename, index=False, header=True)

    meteoblue_df = meteoblue_api.get_meteoblue_forecast_weather_data(township_df)
    filename = f"./output/{str_today}_meteoblue_forecast.csv"
    meteoblue_df.to_csv(filename, index=False, header=True)


if __name__ == "__main__":
    asyncio.run(fetch_weather_data())
