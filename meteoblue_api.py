from datetime import datetime, date
import os
import pandas as pd
import random
import time
from Logger import Logger
from fetch_data import fetch
import json
from dotenv import load_dotenv


class MeteoBlueWeatherAPI:
    def __init__(self):
        self.logger = Logger().get_logger("MeteoBlueWeatherAPI")
        self.print_info("MeteoBlueWeatherAPI is initialized")

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

        today = date.today()
        str_today = today.strftime("%Y-%m-%d")

        all_data = []

        for _, row in township_df.iterrows():
            region_name = row["SR_Name_Eng"]
            district_name = row["District/SAZ_Name_Eng"]
            township_name = row["Township_Name_Eng"]
            town_name = row["Town_Name_Eng"]
            lat = row["Latitude"]
            lon = row["Longitude"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                message = f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}"
                self.print_info(message)
                continue

            message = f"Fetching weather data for Town: {town_name}, Latitude: {lat}, Longitude: {lon}"
            self.print_info(message)

            load_dotenv()
            API_KEY = os.getenv("METEOBLUE_API_KEY")
            url = f"https://my.meteoblue.com/packages/current?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json"

            # Sleep between 1 to 5 seconds
            # time.sleep(random.uniform(1, 3))

            # response = requests.get(url)
            response, status = await fetch(url)

            if status == 429:
                raise ValueError("Available credits exceeded for this API key")

            if status != 200:
                raise ConnectionError(f"Fetch data from meteo-blue API - FAILED ")

            data = json.loads(response)

            current_data = data['data_current']
            current_units = data['units']


            current_weather_list = []


            current_weather_list.append({
                "date" : current_data['time'],
                "date_units" : current_units['time'],
                'state' : region_name,
                'district' : district_name,
                'township' : township_name,
                'latitude': lat,
                'longitude' : lon,
                "isobserveddata" : current_data['isobserveddata'],
                "metarid" : current_data['metarid'],
                'isdaylight' : current_data['isdaylight'],
                'windspeed' : current_data['windspeed'],
                "windspeed_unit" : current_units['windspeed'],
                'zenithangle' : current_data['zenithangle'],
                "pictocode_detailed" : current_data['pictocode_detailed'],
                "pictocode" : current_data['pictocode'],
                "temperature" : current_data['temperature'],
                "temperature_unit" : current_units['temperature'],
                "extraction_date": str_today,  # Add today's date as extraction date
            })

            current_weather_df = pd.DataFrame(current_weather_list)
            all_data.append(current_weather_df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    async def get_meteoblue_forecast_weather_data(
        self, township_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Fetch weather forecast weather data from meteoblue Weather API.

        Parameters:
            lat (float): Latitude
            lon (float): Longitude

        Returns:
            pd.DataFrame: Weather data in DataFrame format
        """

        today = date.today()
        str_today = today.strftime("%Y-%m-%d")

        all_data = []

        for _, row in township_df.iterrows():
            region_name = row["SR_Name_Eng"]
            district_name = row["District/SAZ_Name_Eng"]
            township_name = row["Township_Name_Eng"]
            town_name = row["Town_Name_Eng"]
            lat = row["Latitude"]
            lon = row["Longitude"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                message = f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}"
                self.print_info(message)
                continue

            message = f"Fetching weather data for Township: {township_name}, Latitude: {lat}, Longitude: {lon}"
            self.print_info(message)

            API_KEY = os.getenv("METEOBLUE_API_KEY")
            forecast_days = 7
            url = f"https://my.meteoblue.com/packages/basic-day?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json&forecast_days={forecast_days}"

            # Sleep between 1 to 5 seconds
            # time.sleep(random.uniform(1, 5))

            # response = requests.get(url)
            response, status = await fetch(url)

            data = json.loads(response)

            units = data['units']

            meteo_weather_data = []

            for i in range(len(data["data_day"]["time"])):
                # print(i)
                meteo_weather_data.append({
                    'date' : data['data_day']['time'][i],
                    'date_unit' : units['time'],
                    'state' : region_name,
                    'district' : district_name,
                    'township' : township_name,
                    'town' : town_name,
                    'latitude': lat,
                    'longitude' : lon,
                    'temperature_instant' : data['data_day']['temperature_instant'][i],
                    'temperature_instant_unit' : units['temperature'],
                    'temperature_min' : data['data_day']['temperature_min'][i],
                    'temperature_min_unit' : units['temperature'],
                    'temperature_max' : data['data_day']['temperature_max'][i],
                    'temperature_max_unit' : units['temperature'],
                    'temperature_mean' : data['data_day']['temperature_mean'][i],
                    'temperature_mean_unit' : units['temperature'],
                    'sealevelpressure_min' : data['data_day']['sealevelpressure_min'][i],
                    'sealevelpressure_min_unit' : units['pressure'],
                    'sealevelpressure_max' : data['data_day']['sealevelpressure_max'][i],
                    'sealevelpressure_max_unit' : units['pressure'],
                    'sealevelpressure_mean' : data['data_day']['sealevelpressure_mean'][i],
                    'sealevelpressure_mean_unit' : units['pressure'],
                    'windspeed_min' : data['data_day']['windspeed_min'][i],
                    'windspeed_min_unit' : units['windspeed'],
                    'windspeed_max' : data['data_day']['windspeed_max'][i],
                    'windspeed_max_unit' : units['windspeed'],
                    'windspeed_mean' : data['data_day']['windspeed_mean'][i],
                    'windspeed_mean_unit' : units['windspeed'],
                    'winddirection_degree' : data['data_day']['winddirection'][i],
                    'humiditygreater90_hours' : (data['data_day']['humiditygreater90_hours'][i] / 100),
                    'humiditygreater90_hours_unit' : units['relativehumidity'],
                    'relativehumidity_min' : (data['data_day']['relativehumidity_min'][i] / 100),
                    'relativehumidity_min_unit' : units['relativehumidity'],
                    'relativehumidity_max' : (data['data_day']['relativehumidity_max'][i] / 100),
                    'relativehumidity_max_unit' : units['relativehumidity'],
                    'relativehumidity_mean' : (data['data_day']['relativehumidity_mean'][i] / 100),
                    'relativehumidity_mean_unit' : units['relativehumidity'],
                    'precipitation' : data['data_day']['precipitation'][i],
                    'precipitation_unit' : units['precipitation'],
                    'precipitation_probability' : (data['data_day']['precipitation_probability'][i] / 100),
                    'precipitation_probability_unit' : units['precipitation_probability'],
                    'predictability' : (data['data_day']['predictability'][i] / 100),
                    'predictability_unit' : units['predictability'],
                    'convective_precipitation' : (data['data_day']['convective_precipitation'][i] / 100),
                    'convective_precipitation_unit' : units('precipitation_probability'),
                    'uvindex' : data['data_day']['uvindex'][i],
                    'rainspot' : data['data_day']['rainspot'][i],
                    'predictability_class' : data['data_day']['predictability_class'][i],
                    "extraction_date": str_today,  # Add today's date as extraction date
                })

            meteo_weather_dataFrame = pd.DataFrame(meteo_weather_data)
            all_data.append(meteo_weather_dataFrame)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
