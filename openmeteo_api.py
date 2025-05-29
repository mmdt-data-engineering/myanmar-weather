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
                "tsp_pcode": row["Tsp_Pcode"],
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
                "tsp_pcode": row["Tsp_Pcode"],
                "township": row["Township_Name_Eng"],
                "latitude": row["Latitude"],
                "longitude": row["Longitude"],
            }

            df = await self._get_forecast(township_info)

            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    async def _get_current(self, township_info: dict) -> pd.DataFrame:

        tsp_pcode = township_info["Tsp_Pcode"]
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
            "data_source": "openmeteo",
            "date": pd.to_datetime(current["time"]),
            # Add today's date as extraction date
            "extraction_date": pd.to_datetime(today),
            "tsp_pcode": tsp_pcode,
            "township": township,
            "latitude": response["latitude"],
            "longitude": response["longitude"],
            "elevation": response["elevation"],
            "generationtime_ms": response["generationtime_ms"],
            "utc_offset_seconds": response["utc_offset_seconds"],
            "timezone": response["timezone"],
            "timezone_abbreviation": response["timezone_abbreviation"],
            "interval": current["interval"],
            "interval_units": current_units["interval"],
            "weather_code": current["weather_code"],
            "weather_description": get_weather_description(
                int(current["weather_code"])
            ),
            "weather_code_units": current_units["weather_code"],
            "temperature_2m": current["temperature_2m"],
            "temperature_2m_units": current_units["temperature_2m"],
            "apparent_temperature": current["apparent_temperature"],
            "apparent_temperature_units": current_units["apparent_temperature"],
            "relative_humidity_2m": current["relative_humidity_2m"],
            "relative_humidity_2m_units": current_units["relative_humidity_2m"],
            "is_day": current["is_day"],
            "is_day_units": current_units["is_day"],
            "wind_speed_10m": current["wind_speed_10m"],
            "wind_speed_10m_units": current_units["wind_speed_10m"],
            "wind_direction_10m": current["wind_direction_10m"],
            "wind_direction_10m_units": current_units["wind_direction_10m"],
            "wind_gusts_10m": current["wind_gusts_10m"],
            "wind_gusts_10m_units": current_units["wind_gusts_10m"],
            "precipitation": current["precipitation"],
            "precipitation_units": current_units["precipitation"],
            "showers": current["showers"],
            "showers_units": current_units["showers"],
            "snowfall": current["snowfall"],
            "snowfall_units": current_units["snowfall"],
            "rain": current["rain"],
            "rain_units": current_units["rain"],
            "cloud_cover": current["cloud_cover"],
            "cloud_cover_units": current_units["cloud_cover"],
            "pressure_msl": current["pressure_msl"],
            "pressure_msl_units": current_units["pressure_msl"],
            "surface_pressure": current["surface_pressure"],
            "surface_pressure_units": current_units["surface_pressure"],
        }

        current_list.append(data)

        df = pd.DataFrame(current_list)

        return df

    async def _get_forecast(self, township_info: dict) -> pd.DataFrame:

        tsp_pcode = township_info["tsp_pcode"]
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

        res, status = await fetch(url=url, headers=self.headers, params=params)

        if status != 200:
            raise ConnectionError(f"Fetch data from open-meteo weather API - FAILED ")

        response = json.loads(res)

        forecast = response["daily"]
        forecast_units = response["daily_units"]

        forecast_list = []

        for i in range(len(response["daily"]["time"])):

            data = {
                "data_source": "openmeteo",
                "date": pd.to_datetime(response["daily"]["time"][i]),
                "extraction_date": pd.to_datetime(time.strftime("%Y-%m-%d")),
                "tsp_pcode": tsp_pcode,
                "township": township,
                "latitude": response["latitude"],
                "longitude": response["longitude"],
                "elevation": response["elevation"],
                "generationtime_ms": response["generationtime_ms"],
                "utc_offset_seconds": response["utc_offset_seconds"],
                "timezone": response["timezone"],
                "timezone_abbreviation": response["timezone_abbreviation"],
                "date_units": forecast_units["time"],
                "weather_code": forecast["weather_code"][i],
                "weather_description": get_weather_description(
                    int(forecast["weather_code"][i])
                ),
                "weather_code_units": forecast_units["weather_code"],
                "temperature_2m_max": forecast["temperature_2m_max"][i],
                "temperature_2m_max_units": forecast_units["temperature_2m_max"],
                "temperature_2m_min": forecast["temperature_2m_min"][i],
                "temperature_2m_min_units": forecast_units["temperature_2m_min"],
                "apparent_temperature_max": forecast["apparent_temperature_max"][i],
                "apparent_temperature_max_units": forecast_units[
                    "apparent_temperature_max"
                ],
                "apparent_temperature_min": forecast["apparent_temperature_min"][i],
                "apparent_temperature_min_units": forecast_units[
                    "apparent_temperature_min"
                ],
                "sunrise": forecast["sunrise"][i],
                "sunrise_units": forecast_units["sunrise"],
                "sunset": forecast["sunset"][i],
                "sunset_units": forecast_units["sunset"],
                "daylight_duration": forecast["daylight_duration"][i],
                "daylight_duration_units": forecast_units["daylight_duration"],
                "sunshine_duration": forecast["sunshine_duration"][i],
                "sunshine_duration_units": forecast_units["sunshine_duration"],
                "uv_index_max": forecast["uv_index_max"][i],
                "uv_index_max_units": forecast_units["uv_index_max"],
                "uv_index_clear_sky_max": forecast["uv_index_clear_sky_max"][i],
                "uv_index_clear_sky_max_units": forecast_units[
                    "uv_index_clear_sky_max"
                ],
                "rain_sum": forecast["rain_sum"][i],
                "rain_sum_units": forecast_units["rain_sum"],
                "showers_sum": forecast["showers_sum"][i],
                "showers_sum_units": forecast_units["showers_sum"],
                "snowfall_sum": forecast["snowfall_sum"][i],
                "snowfall_sum_units": forecast_units["snowfall_sum"],
                "precipitation_sum": forecast["precipitation_sum"][i],
                "precipitation_sum_units": forecast_units["precipitation_sum"],
                "precipitation_hours": forecast["precipitation_hours"][i],
                "precipitation_hours_units": forecast_units["precipitation_hours"],
                "precipitation_probability_max": forecast[
                    "precipitation_probability_max"
                ][i],
                "precipitation_probability_max_units": forecast_units[
                    "precipitation_probability_max"
                ],
                "wind_speed_10m_max": forecast["wind_speed_10m_max"][i],
                "wind_speed_10m_max_units": forecast_units["wind_speed_10m_max"],
                "wind_gusts_10m_max": forecast["wind_gusts_10m_max"][i],
                "wind_gusts_10m_max_units": forecast_units["wind_gusts_10m_max"],
                "wind_direction_10m_dominant": forecast["wind_direction_10m_dominant"][
                    i
                ],
                "wind_direction_10m_dominant_units": forecast_units[
                    "wind_direction_10m_dominant"
                ],
                # mjm2 = mega joules per square meter
                "shortwave_radiation_sum": forecast["shortwave_radiation_sum"][i],
                "shortwave_radiation_sum_units": forecast_units[
                    "shortwave_radiation_sum"
                ],
                "et0_fao_evapotranspiration": forecast["et0_fao_evapotranspiration"][i],
                "et0_fao_evapotranspiration_units": forecast_units[
                    "et0_fao_evapotranspiration"
                ],
            }

            forecast_list.append(data)

        df = pd.DataFrame(forecast_list)

        return df
