from datetime import datetime
import os
import pandas as pd
import random
import requests
import time
from Logger import Logger
<<<<<<< HEAD
=======

>>>>>>> 3bf7aa72bf8bfc2b3b60fcb9fd2bbc543ddb9b97

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

    def get_meteoblue_current_weather_data(
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

<<<<<<< HEAD
            info = f"Fetching weather data for Town: {town_name}, Latitude: {lat}, Longitude: {lon}"
            self.logger.info(info)
=======
            message = f"Fetching weather data for Township: {township_name}, Latitude: {lat}, Longitude: {lon}"
            self.print_info(message)
>>>>>>> 3bf7aa72bf8bfc2b3b60fcb9fd2bbc543ddb9b97

            API_KEY = os.getenv("meteoblue_api_key")
            # url = f"https://my.meteoblue.com/packages/basic-day_current?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json"
            url = f"https://my.meteoblue.com/packages/current?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json"
            time.sleep(random.uniform(1, 5))  # Sleep between 1 to 5 seconds

            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                current_weather_list = []

<<<<<<< HEAD

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
=======
                for k, v in data["data_current"].items():
                    current_weather_list.append(
                        {
                            "date": pd.to_datetime(data["data_current"]["time"]),
                            "isobserveddata": data["data_current"]["isobserveddata"],
                            "metarid": data["data_current"]["metarid"],
                            "isdaylight": data["data_current"]["isdaylight"],
                            "windspeed": data["data_current"]["windspeed"],
                            "zenithangle": data["data_current"]["zenithangle"],
                            "pictocode_detailed": data["data_current"][
                                "pictocode_detailed"
                            ],
                            "pictocode": data["data_current"]["pictocode"],
                            "temperature": data["data_current"]["temperature"],
                        }
                    )
>>>>>>> 3bf7aa72bf8bfc2b3b60fcb9fd2bbc543ddb9b97

                current_weather_df = pd.DataFrame(current_weather_list)
                all_data.append(current_weather_df)

            except requests.RequestException as e:
<<<<<<< HEAD
                err = f"[ERROR] Failed to fetch data for ({lat}, {lon}): {e}"
                self.logger.info(err)
=======
                error_message = f"Error fetching data due to {e}"
                print(error_message)
                self.logger.error(error_message)
>>>>>>> 3bf7aa72bf8bfc2b3b60fcb9fd2bbc543ddb9b97

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

<<<<<<< HEAD
    def get_meteoblue_forecast_weather_data(self, township_df: pd.DataFrame) -> pd.DataFrame:
=======
    def get_meteoblue_forecast_weather_data(
        self, township_df: pd.DataFrame
    ) -> pd.DataFrame:
>>>>>>> 3bf7aa72bf8bfc2b3b60fcb9fd2bbc543ddb9b97
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

            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                meteo_weather_data = []

<<<<<<< HEAD
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
=======
                for k, v in data.items():
                    # print(k, '\n', v)
                    for col, val in data["data_day"].items():
                        # print(col, '\n', val)
                        for i in range(len(val)):
                            # print(i)
                            meteo_weather_data.append(
                                {
                                    "date": data["data_day"]["time"][i],
                                    "latitude": data["metadata"]["latitude"],
                                    "longitude": data["metadata"]["latitude"],
                                    "temperature_instant_°C": data["data_day"][
                                        "temperature_instant"
                                    ][i],
                                    "precipitation_mm": data["data_day"][
                                        "precipitation"
                                    ][i],
                                    "predictability_percent": data["data_day"][
                                        "predictability"
                                    ][i],
                                    "temperature_min_°C": data["data_day"][
                                        "temperature_min"
                                    ][i],
                                    "temperature_max_°C": data["data_day"][
                                        "temperature_max"
                                    ][i],
                                    "temperature_mean_°C": data["data_day"][
                                        "temperature_mean"
                                    ][i],
                                    "sealevelpressure_min_hPa": data["data_day"][
                                        "sealevelpressure_min"
                                    ][i],
                                    "sealevelpressure_max_hPa": data["data_day"][
                                        "sealevelpressure_max"
                                    ][i],
                                    "sealevelpressure_mean_hPa": data["data_day"][
                                        "sealevelpressure_mean"
                                    ][i],
                                    "windspeed_min_ms-1": data["data_day"][
                                        "windspeed_min"
                                    ][i],
                                    "windspeed_max_ms-1": data["data_day"][
                                        "windspeed_max"
                                    ][i],
                                    "windspeed_mean_ms-1": data["data_day"][
                                        "windspeed_mean"
                                    ][i],
                                    "humiditygreater90_hours_percent": data["data_day"][
                                        "humiditygreater90_hours"
                                    ][i],
                                    "convective_precipitation_percent": data[
                                        "data_day"
                                    ]["convective_precipitation"][i],
                                    "relativehumidity_min_percent": data["data_day"][
                                        "relativehumidity_min"
                                    ][i],
                                    "relativehumidity_max_percent": data["data_day"][
                                        "relativehumidity_max"
                                    ][i],
                                    "relativehumidity_mean_percent": data["data_day"][
                                        "relativehumidity_mean"
                                    ][i],
                                    "winddirection_degree": data["data_day"][
                                        "winddirection"
                                    ][i],
                                    "precipitation_probability_percent": data[
                                        "data_day"
                                    ]["precipitation_probability"][i],
                                    "uvindex": data["data_day"]["uvindex"][i],
                                    "rainspot": data["data_day"]["rainspot"][i],
                                    "predictability_class": data["data_day"][
                                        "predictability_class"
                                    ][i],
                                }
                            )
>>>>>>> 3bf7aa72bf8bfc2b3b60fcb9fd2bbc543ddb9b97

                meteo_weather_dataFrame = pd.DataFrame(meteo_weather_data)
                all_data.append(meteo_weather_dataFrame)

            except requests.RequestException as e:
                message = f"Error fetching data due to {e}"
                print(message)
                self.logger.error(message)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
