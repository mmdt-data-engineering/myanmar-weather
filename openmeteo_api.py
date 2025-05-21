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

        data = {
            "date": pd.to_datetime(current["time"]),
            "extration_date": pd.to_datetime(time.strftime("%Y-%m-%d")),
            "region": region,
            "district": district,
            "township": township,
            "latitude": response["latitude"],
            "longitude": response["longitude"],
            "elevation": response["elevation"],
            "interval_second": current["interval"],
            "temperature_2m_celsius": current["temperature_2m"],
            "relative_humidity_2m_percent": current["relative_humidity_2m"],
            "apparent_temperature_celsius": current["apparent_temperature"],
            "is_day": current["is_day"],
            "wind_speed_10m_kmh": current["wind_speed_10m"],
            "wind_direction_10m_degree": current["wind_direction_10m"],
            "wind_gusts_10m_kmh": current["wind_gusts_10m"],
            "precipitation_mm": current["precipitation"],
            "showers_mm": current["showers"],
            "snowfall_cm": current["snowfall"],
            "rain_mm": current["rain"],
            "weather_code": current["weather_code"],
            "cloud_cover_percent": current["cloud_cover"],
            "pressure_msl_hectopascal": current["pressure_msl"],
            "surface_pressure_hectopascal": current["surface_pressure"],
            # "generationtime_ms": response["generationtime_ms"],
            # "utc_offset_seconds": response["utc_offset_seconds"],
            # "timezone": response["timezone"],
            # "timezone_abbreviation": response["timezone_abbreviation"],
            # "interval_units": current_units["interval"],
            # "temperature_2m_units": current_units["temperature_2m"],
            # "relative_humidity_2m_units": current_units["relative_humidity_2m"],
            # "apparent_temperature_units": current_units["apparent_temperature"],
            # "is_day_units": current_units["is_day"],
            # "wind_speed_10m_units": current_units["wind_speed_10m"],
            # "wind_direction_10m_units": current_units["wind_direction_10m"],
            # "wind_gusts_10m_units": current_units["wind_gusts_10m"],
            # "precipitation_units": current_units["precipitation"],
            # "showers_units": current_units["showers"],
            # "snowfall_units": current_units["snowfall"],
            # "rain_units": current_units["rain"],
            # "weather_code_units": current_units["weather_code"],
            # "cloud_cover_units": current_units["cloud_cover"],
            # "pressure_msl_units": current_units["pressure_msl"],
            # "surface_pressure_units": current_units["surface_pressure"],
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
                "temperature_2m_max_celsius": daily["temperature_2m_max"][i],
                "temperature_2m_min_celsius": daily["temperature_2m_min"][i],
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
                "rain_sum_mm": daily["rain_sum"][i],
                "showers_sum_mm": daily["showers_sum"][i],
                "snowfall_sum_cm": daily["snowfall_sum"][i],
                "precipitation_sum_mm": daily["precipitation_sum"][i],
                "precipitation_hours": daily["precipitation_hours"][i],
                "precipitation_probability_max_percent": daily[
                    "precipitation_probability_max"
                ][i],
                "wind_speed_10m_max_kmh": daily["wind_speed_10m_max"][i],
                "wind_gusts_10m_max_kmh": daily["wind_gusts_10m_max"][i],
                "wind_direction_10m_dominant_degree": daily[
                    "wind_direction_10m_dominant"
                ][i],
                "shortwave_radiation_sum_mjm2": daily["shortwave_radiation_sum"][i],
                "et0_fao_evapotranspiration_mm": daily["et0_fao_evapotranspiration"][i],
                # "date_units": daily_units["time"],
                # "generationtime_ms": response["generationtime_ms"],
                # "utc_offset_seconds": response["utc_offset_seconds"],
                # "timezone": response["timezone"],
                # "timezone_abbreviation": response["timezone_abbreviation"],
                # "weather_code_units": daily_units["weather_code"],
                # "temperature_2m_max_units": daily_units["temperature_2m_max"],
                # "temperature_2m_min_units": daily_units["temperature_2m_min"],
                # "apparent_temperature_max_units": daily_units[
                # "apparent_temperature_max"
                # ],
                # "apparent_temperature_min_units": daily_units[
                #     "apparent_temperature_min"
                # ],
                # "sunrise_units": daily_units["sunrise"],
                # "sunset_units": daily_units["sunset"],
                # "daylight_duration_units": daily_units["daylight_duration"],
                # "sunshine_duration_units": daily_units["sunshine_duration"],
                # "uv_index_max_units": daily_units["uv_index_max"],
                # "uv_index_clear_sky_max_units": daily_units["uv_index_clear_sky_max"],
                # "rain_sum_units": daily_units["rain_sum"],
                # "showers_sum_units": daily_units["showers_sum"],
                # "snowfall_sum_units": daily_units["snowfall_sum"],
                # "precipitation_sum_units": daily_units["precipitation_sum"],
                # "precipitation_hours_units": daily_units["precipitation_hours"],
                # "precipitation_probability_max_units": daily_units[
                #     "precipitation_probability_max"
                # ],
                # "wind_speed_10m_max_units": daily_units["wind_speed_10m_max"],
                # "wind_gusts_10m_max_units": daily_units["wind_gusts_10m_max"],
                # "wind_direction_10m_dominant_units": daily_units[
                #     "wind_direction_10m_dominant"
                # ],
                # "shortwave_radiation_sum_units": daily_units["shortwave_radiation_sum"],
                # "et0_fao_evapotranspiration_units": daily_units[
                #     "et0_fao_evapotranspiration"
                # ],
            }

            daily_list.append(data)

        df = pd.DataFrame(daily_list)

        return df
