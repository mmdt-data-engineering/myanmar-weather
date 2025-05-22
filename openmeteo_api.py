import openmeteo_requests
import pandas as pd
import requests_cache
import time, random
from retry_requests import retry
import time
from openmeteo_attributes import current_attributes, daily_attributes
from Logger import Logger
import requests
import json
from fetch_data import fetch
from data_utils import get_weather_description
from datetime import date


class OpenMeteoAPI:

    def __init__(self):
        self.cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=self.retry_session)

        self.headers = {
            "User-Agent": "Chrome/135.0.0.0 Safari/537.36",
            "Referer": "https://open-meteo.com/",
            "Origin": "https://open-meteo.com/",
            "Accept": "application/json",
        }

        self.logger = Logger().get_logger("OpenMeteoAPI")
        self.print_info("OpenMeteoAPI is initialized")

    def print_info(self, message):
        """
        Prints the log message to the console and logs it.
        """
        print(message)
        self.logger.info(message)

    async def get_current(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetches hourly weather data for the specified townships by using latitude and longitude.

        Note: Current conditions are based on 15-minutely weather model data. Every weather variable available in hourly data,
        is available as current condition as well.
        """

        result_df = pd.DataFrame()

        for index, row in df.iterrows():

            township_info = {
                "region": row["SR_Name_Eng"],
                "district": row["District/SAZ_Name_Eng"],
                "township": row["Township_Name_Eng"],
                "latitude": row["Latitude"],
                "longitude": row["Longitude"],
            }

            df = await self._get_current(township_info)

            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    async def get_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        result_df = pd.DataFrame()

        for index, row in df.iterrows():

            township_info = {
                "region": row["SR_Name_Eng"],
                "district": row["District/SAZ_Name_Eng"],
                "township": row["Township_Name_Eng"],
                "latitude": row["Latitude"],
                "longitude": row["Longitude"],
            }

            df = await self._get_daily(township_info)

            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    async def _get_current(self, township_info: dict) -> pd.DataFrame:

        region = township_info["region"]
        district = township_info["district"]
        township = township_info["township"]
        latitude = township_info["latitude"]
        longitude = township_info["longitude"]

        message = f"Township: {township}, Latitude: {latitude}, Longitude: {longitude}"
        self.print_info(message)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": current_attributes,
        }

        # responses = openmeteo.weather_api(url, params=params)
        # res = requests.get(url, headers=self.headers, params=params, timeout=10)
        res, status = await fetch(url=url, headers=self.headers, params=params)

        if status != 200:
            raise ConnectionError(f"Fetch data from open-meteo weather API - FAILED ")

        response = json.loads(res)

        current_units = response["current_units"]
        current = response["current"]

        current_list = []

        today = date.today().strftime("%Y-%m-%d")

        data = {
            "date": pd.to_datetime(current["time"]),
            # Add today's date as extraction date
            "extration_date": pd.to_datetime(today),
            "state": region,
            "district": district,
            "township": township,
            "latitude": response["latitude"],
            "longitude": response["longitude"],
            "elevation": response["elevation"],
            "interval_second": current["interval"],
            "weather_code": current["weather_code"],
            "weather_description": get_weather_description(
                int(current["weather_code"])
            ),
            "temperature_celsius": current["temperature_2m"],
            "relative_humidity_percent": current["relative_humidity_2m"],
            "apparent_temperature_celsius": current["apparent_temperature"],
            "is_day": current["is_day"],
            "wind_speed_kilometers_per_hour": current["wind_speed_10m"],
            "wind_direction_degree": current["wind_direction_10m"],
            "wind_gusts_kilometers_per_hour": current["wind_gusts_10m"],
            "precipitation_millimeters": current["precipitation"],
            "showers_millimeters": current["showers"],
            "snowfall_centimeters": current["snowfall"],
            "rain_millimeters": current["rain"],
            "cloud_cover_percent": current["cloud_cover"],
            "pressure_msl_hectopascals": current["pressure_msl"],
            "surface_pressure_hectopascals": current["surface_pressure"],
        }

        current_list.append(data)

        df = pd.DataFrame(current_list)

        return df

    async def _get_daily(self, township_info: dict) -> pd.DataFrame:

        region = township_info["region"]
        district = township_info["district"]
        township = township_info["township"]
        latitude = township_info["latitude"]
        longitude = township_info["longitude"]

        message = f"Township: {township}, Latitude: {latitude}, Longitude: {longitude}"
        self.print_info(message)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": daily_attributes,
            # "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min"],
        }
        # responses = openmeteo.weather_api(url, params=params)
        # res = requests.get(url, headers=self.headers, params=params, timeout=10)
        res, status = await fetch(url=url, headers=self.headers, params=params)

        if status != 200:
            raise ConnectionError(f"Fetch data from open-meteo weather API - FAILED ")

        response = json.loads(res)

        daily = response["daily"]
        daily_units = response["daily_units"]

        daily_list = []

        for i in range(len(response["daily"]["time"])):

            data = {
                "date": pd.to_datetime(response["daily"]["time"][i]),
                "extraction_date": pd.to_datetime(time.strftime("%Y-%m-%d")),
                "region": region,
                "district": district,
                "township": township,
                "latitude": response["latitude"],
                "longitude": response["longitude"],
                "elevation": response["elevation"],
                "weather_code": daily["weather_code"][i],
                "weather_description": get_weather_description(
                    int(daily["weather_code"][i])
                ),
                "temperature_max_celsius": daily["temperature_2m_max"][i],
                "temperature_min_celsius": daily["temperature_2m_min"][i],
                "apparent_temperature_max_celsius": daily["apparent_temperature_max"][
                    i
                ],
                "apparent_temperature_min_celsius": daily["apparent_temperature_min"][
                    i
                ],
                "sunrise_time": daily["sunrise"][i],
                "sunset_time": daily["sunset"][i],
                "daylight_duration_second": daily["daylight_duration"][i],
                "sunshine_duration_second": daily["sunshine_duration"][i],
                "uv_index_max": daily["uv_index_max"][i],
                "uv_index_clear_sky_max": daily["uv_index_clear_sky_max"][i],
                "rain_sum_millimeters": daily["rain_sum"][i],
                "showers_sum_millimeters": daily["showers_sum"][i],
                "snowfall_sum_centimeters": daily["snowfall_sum"][i],
                "precipitation_sum_millimeters": daily["precipitation_sum"][i],
                "precipitation_hours": daily["precipitation_hours"][i],
                "precipitation_probability_max_percent": daily[
                    "precipitation_probability_max"
                ][i],
                "wind_speed_max_kilometers_per_hour": daily["wind_speed_10m_max"][i],
                "wind_gusts_max_kilometers_per_hour": daily["wind_gusts_10m_max"][i],
                "wind_direction_dominant_degree": daily["wind_direction_10m_dominant"][
                    i
                ],
                # mjm2 = mega joules per square meter
                "shortwave_radiation_sum_mjm2": daily["shortwave_radiation_sum"][i],
                "et0_fao_evapotranspiration_milimeters": daily[
                    "et0_fao_evapotranspiration"
                ][i],
            }

            daily_list.append(data)

        df = pd.DataFrame(daily_list)

        return df
