import openmeteo_requests
import pandas as pd
import requests_cache
import time, random
from retry_requests import retry
from openmeteo_attributes import hourly_attributes, daily_attributes
from Logger import Logger


class OpenMeteoAPI:

    def __init__(self):
        self.cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=self.retry_session)
        self.logger = Logger().get_logger("OpenMeteoAPI")
        self.print_info("OpenMeteoAPI is initialized")

    def print_info(self, message):
        """
        Prints the log message to the console and logs it.
        """
        print(message)
        self.logger.info(message)

    def get_current(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetches hourly weather data for the specified townships by using latitude and longitude.

        Note: Current conditions are based on 15-minutely weather model data. Every weather variable available in hourly data,
        is available as current condition as well.
        """

        result_df = pd.DataFrame()

        for index, row in df.iterrows():
            township_name = row["Township_Name_Eng"]
            latitude = row["Latitude"]
            longitude = row["Longitude"]

            message = f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
            self.print_info(message)

            # Random sleep time between 1 and 5 seconds
            sleep_time = random.uniform(1, 5)

            message = f"Sleeping for {sleep_time:.2f} seconds..."
            self.print_info(message)

            df = self._get_current(latitude, longitude)
            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    def get_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        result_df = pd.DataFrame()

        for index, row in df.iterrows():

            township_name = row["Township_Name_Eng"]
            latitude = row["Latitude"]
            longitude = row["Longitude"]

            message = f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
            self.print_info(message)

            # Random sleep time between 1 and 5 seconds
            sleep_time = random.uniform(1, 5)

            message = f"Sleeping for {sleep_time:.2f} seconds..."
            self.print_info(message)

            df = self._get_daily(latitude, longitude)
            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    def _get_current(self, latitude: float, longitude: float) -> pd.DataFrame:

        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": hourly_attributes,
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]

        message = f"Coordinates {response.Latitude()}°N {response.Longitude()}°E"
        self.print_info(message)
        message = f"Elevation {response.Elevation()} m asl"
        self.print_info(message)
        message = f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}"
        self.print_info(message)
        message = f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s"
        self.print_info(message)

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }

        for i, attribute in enumerate(hourly_attributes):
            self.print_info(f"{attribute}: {hourly.Variables(i).ValuesAsNumpy()}")
            hourly_data[str(attribute)] = hourly.Variables(i).ValuesAsNumpy()

        df = pd.DataFrame(data=hourly_data)

        return df

    def _get_daily(self, latitude: float, longitude: float) -> pd.DataFrame:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": daily_attributes,
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]

        message = f"Coordinates {response.Latitude()}°N {response.Longitude()}°E"
        self.print_info(message)
        message = f"Elevation {response.Elevation()} m asl"
        self.print_info(message)
        message = f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}"
        self.print_info(message)
        message = f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s"
        self.print_info(message)

        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()

        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left",
            )
        }

        for i, attribute in enumerate(daily_attributes):
            daily_data[str(attribute)] = daily.Variables(i).ValuesAsNumpy()

        df = pd.DataFrame(data=daily_data)

        return df
