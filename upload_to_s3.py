from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI
from ambientweather_api import AmbientWeatherAPI
from datetime import date
import os
from cloud_utils import AWS


def fetch_weather_data():
    # Get townships from MIMU data
    mimu_data = MIMU_Data()
    township_df = mimu_data.get_townships()

    township_df = township_df.head(1)

    weather_api = WeatherAPI()

    today = date.today()
    str_today = today.strftime("%Y-%m-%d")  # Output like '2025-05-16'

    # weatherapi - current
    weatherapi_current_df = weather_api.get_current(township_df)
    filename = f"./output/{str_today}_weatherapi_current.csv"
    weatherapi_current_df.to_csv(filename, index=False, header=True)

    # weatherapi - daily
    weatherapi_daily_df = weather_api.get_daily(township_df, no_of_days=7)
    filename = f"./output/{str_today}_weatherapi_daily.csv"
    weatherapi_daily_df.to_csv(filename, index=False, header=True)

    openmeteo_api = OpenMeteoAPI()

    # open-meteo - current
    openmeteo_current_df = openmeteo_api.get_current(township_df)
    filename = f"./output/{str_today}_open_meteo_current.csv"
    openmeteo_current_df.to_csv(filename, index=False, header=True)

    # open-meteo - daily
    openmeteo_daily_df = openmeteo_api.get_daily(township_df)
    filename = f"./output/{str_today}_open_meteo_daily.csv"
    openmeteo_daily_df.to_csv(filename, index=False, header=True)

    # Fetch AmbientWeather data
    ambient_api = AmbientWeatherAPI()  # New instance for AmbientWeatherAPI

    ambient_df = ambient_api.get_forecast_df(township_df)  # Get forecast data
    filename = f"./output/{str_today}_ambientweather_data.csv"
    ambient_df.to_csv(filename, index=False, header=True)


def upload_to_s3():

    # Initialize CloudUtils
    aws_client = AWS()

    # Set the directory containing the CSV files
    folder_path = "./output"

    if not os.path.exists(folder_path):
        raise FileNotFoundError(
            f"Folder not found: {folder_path} (working dir: {os.getcwd()})"
        )

    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    print(f"CSV files found: {csv_files}")

    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)

        try:
            aws_client.upload_file(file_path)
        except Exception as e:
            print(f"Error uploading {file_name}: {e}")
            continue


if __name__ == "__main__":
    # fetch_weather_data()
    upload_to_s3()
