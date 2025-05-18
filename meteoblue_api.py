from datetime import datetime
import os
import pandas as pd
import random
import time
from Logger import Logger
from fetch_data import fetch
import json

class MeteoBlueWeatherAPI:
    def __init__(self):
        self.logger = Logger().get_logger("MeteoBlueWeatherAPI")
        self.logger.info("MeteoBlueWeatherAPI is initialized")

    def print_info(self, message):
        """
        Prints the log message to the console and logs it.
        """
        print(message)
        self.logger.info(message)

    async def get_meteoblue_current_weather_data(
        self, township_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
            Fetch weather current weather data from meteoblue Weather API.

            Parameters:
                lat (float): Latitude
                lon (float): Longitude

            Returns:
                pd.DataFrame: Weather data in DataFrame format

        """

        all_data = []

        for _, row in township_df.iterrows():
            region_name = row['SR_Name_Eng']
            district_name = row['District/SAZ_Name_Eng']
            township_name = row["Township_Name_Eng"]
            town_name = row['Town_Name_Eng']
            lat = row["Latitude"]
            lon = row["Longitude"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                message = f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}"
                self.print_info(message)
                continue

            message = f"Fetching weather data for Town: {town_name}, Latitude: {lat}, Longitude: {lon}"
            self.logger.info(message)

            API_KEY = os.getenv("meteoblue_api_key")
            url = f"https://my.meteoblue.com/packages/current?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json"
            time.sleep(random.uniform(1, 5))  # Sleep between 1 to 5 seconds

            # response = requests.get(url)
            response = await fetch(url)

            data = json.loads(response)

            current_weather_list = []


            current_weather_list.append({
                "date" : pd.to_datetime(data['data_current']['time']),
                'region' : region_name,
                'district' : district_name,
                'township' : township_name,
                'town' : town_name,
                'latitude': lat,
                'longitude' : lon,
                "isobserveddata" : data['data_current']['isobserveddata'],
                "metarid" : data['data_current']['metarid'],
                'isdaylight' : data['data_current']['isdaylight'],
                'windspeed' : data['data_current']['windspeed'],
                'zenithangle' : data['data_current']['zenithangle'],
                "pictocode_detailed" : data['data_current']['pictocode_detailed'],
                "pictocode" : data['data_current']['pictocode'],
                "temperature" : data['data_current']['temperature'],
            })

            current_weather_df = pd.DataFrame(current_weather_list)
            all_data.append(current_weather_df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    async def get_meteoblue_forecast_weather_data(self, township_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetch weather forecast weather data from meteoblue Weather API.

        Parameters:
            lat (float): Latitude
            lon (float): Longitude

        Returns:
            pd.DataFrame: Weather data in DataFrame format
        """

        all_data = []

        for _, row in township_df.iterrows():
            region_name = row['SR_Name_Eng']
            district_name = row['District/SAZ_Name_Eng']
            township_name = row["Township_Name_Eng"]
            town_name = row['Town_Name_Eng']
            lat = row["Latitude"]
            lon = row["Longitude"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                message = f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}"
                self.print_info(message)
                continue

            message = f"Fetching weather data for Township: {township_name}, Latitude: {lat}, Longitude: {lon}"
            self.print_info(message)

            API_KEY = os.getenv("meteoblue_api_key")
            forecast_days = 7
            url = f"https://my.meteoblue.com/packages/basic-day?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json&forecast_days={forecast_days}"
            time.sleep(random.uniform(1, 5))  # Sleep between 1 to 5 seconds

            # response = requests.get(url)
            response = await fetch(url)

            data = json.loads(response)

            meteo_weather_data = []

            for i in range(len(data['data_day']['time'])):
                # print(i)
                meteo_weather_data.append({
                    'date' : data['data_day']['time'][i],
                    'region' : region_name,
                    'district' : district_name,
                    'township' : township_name,
                    'town' : town_name,
                    'latitude': lat,
                    'longitude' : lon,
                    'temperature_instant_°C' : data['data_day']['temperature_instant'][i],
                    'precipitation_mm' : data['data_day']['precipitation'][i],
                    'predictability_percent' : data['data_day']['predictability'][i],
                    'temperature_min_°C' : data['data_day']['temperature_min'][i],
                    'temperature_max_°C' : data['data_day']['temperature_max'][i],
                    'temperature_mean_°C' : data['data_day']['temperature_mean'][i],
                    'sealevelpressure_min_hPa' : data['data_day']['sealevelpressure_min'][i],
                    'sealevelpressure_max_hPa' : data['data_day']['sealevelpressure_max'][i],
                    'sealevelpressure_mean_hPa' : data['data_day']['sealevelpressure_mean'][i],
                    'windspeed_min_ms-1' : data['data_day']['windspeed_min'][i],
                    'windspeed_max_ms-1' : data['data_day']['windspeed_max'][i],
                    'windspeed_mean_ms-1' : data['data_day']['windspeed_mean'][i],
                    'humiditygreater90_hours_percent' : data['data_day']['humiditygreater90_hours'][i],
                    'convective_precipitation_percent' : data['data_day']['convective_precipitation'][i],
                    'relativehumidity_min_percent' : data['data_day']['relativehumidity_min'][i],
                    'relativehumidity_max_percent' : data['data_day']['relativehumidity_max'][i],
                    'relativehumidity_mean_percent' : data['data_day']['relativehumidity_mean'][i],
                    'winddirection_degree' : data['data_day']['winddirection'][i],
                    'precipitation_probability_percent' : data['data_day']['precipitation_probability'][i],
                    'uvindex' : data['data_day']['uvindex'][i],
                    'rainspot' : data['data_day']['rainspot'][i],
                    'predictability_class' : data['data_day']['predictability_class'][i],
                })

            meteo_weather_dataFrame = pd.DataFrame(meteo_weather_data)
            all_data.append(meteo_weather_dataFrame)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
