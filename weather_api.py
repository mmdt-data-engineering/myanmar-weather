import pandas as pd
from dotenv import load_dotenv
import os, time, random
from Logger import Logger
from fetch_data import fetch
import json


class WeatherAPI:
    def __init__(self):
        self.logger = Logger().get_logger("WeatherAPI")
        self.logger.info("WeatherAPI is initialized")

    def print_info(self, message):
        """
        Prints the log message to the console and logs it.
        """
        print(message)
        self.logger.info(message)

    async def get_current(self, township_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetches current weather data for the specified townships by using latitude and longitude.
        """

        BASE_URL = "https://api.weatherapi.com/v1/current.json"

        load_dotenv()
        WEATHER_API_KEY = os.getenv("WEATHER_API_COM_KEY")
        if WEATHER_API_KEY is None:
            raise ValueError(
                "API key not found. Please set the WEATHER_API_COM_KEY environment variable."
            )

        result_df = pd.DataFrame()

        for index, row in township_df.iterrows():

            df = await self.get_current_data_from_api(BASE_URL, WEATHER_API_KEY, row)
            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    async def get_current_data_from_api(self, BASE_URL, WEATHER_API_KEY, row):
        township_name = row["Township_Name_Eng"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        town_name = row["Town_Name_Eng"]
        district_name = row["District/SAZ_Name_Eng"]
        state_name = row["SR_Name_Eng"]

        message = f"TownName: {town_name}, Latitude: {latitude}, Longitude: {longitude}"
        self.print_info(message)

        # Construct the API request URL
        url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={latitude},{longitude}"

        # Random sleep time between 1 and 5 seconds
        sleep_time = random.uniform(1, 5)
        message = f"Sleeping for {sleep_time:.2f} seconds..."
        self.print_info(message)
        time.sleep(sleep_time)

        # response = requests.get(url)
        response = await fetch(url)

        data = json.loads(response)

        location = data["location"]
        current = data["current"]
        condition = data["current"]["condition"]

        current_list = []
        current_list.append(
            {
                "town": location.get("name", None),
                "region": location.get("region", None),
                "country": location.get("country", None),
                "latitude": location.get("lat", None),
                "longitude": location.get("lon", None),
                "township": township_name,
                "town_name": town_name,
                "district_name": district_name,
                "state_name": state_name,
                "timezone": location.get("tz_id", None),
                "localtime": location.get("localtime", None),
                "localtime_epoch": location.get("localtime_epoch", None),
                "last_updated": current.get("last_updated", None),
                "last_updated_epoch": current.get("last_updated_epoch", None),
                "temp_c": current.get("temp_c", None),
                "temp_f": current.get("temp_f", None),
                "is_day": current.get("is_day", None),
                "condition": condition.get("text", None),
                "icon": condition.get("icon", None),
                "wind_mph": current.get("wind_mph", None),
                "wind_kph": current.get("wind_kph", None),
                "wind_degree": current.get("wind_degree", None),
                "wind_dir": current.get("wind_dir", None),
                "pressure_mb": current.get("pressure_mb", None),
                "pressure_in": current.get("pressure_in", None),
                "precip_mm": current.get("precip_mm", None),
                "precip_in": current.get("precip_in", None),
                "humidity": current.get("humidity", None),
                "cloud": current.get("cloud", None),
                "feelslike_c": current.get("feelslike_c", None),
                "feelslike_f": current.get("feelslike_f", None),
                "windchill_c": current.get("windchill_c", None),
                "windchill_f": current.get("windchill_f", None),
                "heatindex_c": current.get("heatindex_c", None),
                "heatindex_f": current.get("heatindex_f", None),
                "dewpoint_c": current.get("dewpoint_c", None),
                "dewpoint_f": current.get("dewpoint_f", None),
                "vis_km": current.get("vis_km", None),
                "vis_miles": current.get("vis_miles", None),
                "uv": current.get("uv", None),
                "gust_mph": current.get("gust_mph", None),
                "gust_kph": current.get("gust_kph", None),
            }
        )

        df = pd.DataFrame(current_list)
        return df

    async def get_daily(
        self, township_df: pd.DataFrame, no_of_days: int
    ) -> pd.DataFrame:
        """
        Fetches current weather data for the specified townships by using latitude and longitude.
        """
        # Define the base URL for the weather API
        BASE_URL = "https://api.weatherapi.com/v1/forecast.json"

        # Load environment variables
        load_dotenv()
        WEATHER_API_KEY = os.getenv("WEATHER_API_COM_KEY")
        if WEATHER_API_KEY is None:
            raise ValueError(
                "API key not found. Please set the WEATHER_API_KEY environment variable."
            )

        result_df = pd.DataFrame()

        for index, row in township_df.iterrows():
            df = await self.get_daily_data_from_api(BASE_URL, WEATHER_API_KEY, row)
            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

    async def get_daily_data_from_api(
        self, BASE_URL, WEATHER_API_KEY, row
    ) -> pd.DataFrame:

        NO_OF_DAYS = 7
        township_name = row["Township_Name_Eng"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        town_name = row["Town_Name_Eng"]
        district_name = row["District/SAZ_Name_Eng"]
        state_name = row["SR_Name_Eng"]

        message = f"Town Name:{town_name}, Latitude:{latitude}, Longitude:{longitude}"
        self.print_info(message)

        # Construct the API request URL
        url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={latitude},{longitude}&days={NO_OF_DAYS}&aqi=no&alerts=no"

        sleep_time = random.uniform(1, 5)

        self.print_info(f"Sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

        # response = requests.get(url)
        response = await fetch(url)

        data = json.loads(response)

        # Location
        location = data["location"]
        location_df = pd.json_normalize(location)

        # Forecast
        forecast_data = data["forecast"]["forecastday"]
        df = pd.json_normalize(forecast_data)

        # Select only required columns
        forecast_df = df[
            ["date", "date_epoch"]
            + [col for col in df.columns if col.startswith("day.")]
        ]

        # Repeat location for each forecast row
        location_repeated = pd.concat(
            [location_df] * len(forecast_df), ignore_index=True
        )

        # Combine location and forecast
        final_df = pd.concat([forecast_df, location_repeated], axis=1)

        # remove the prefix from column names
        final_df.columns = final_df.columns.str.replace("day.", "")
        final_df.columns = final_df.columns.str.replace("condition.", "")

        final_df["town_name"] = town_name
        final_df["district_name"] = district_name
        final_df["state_name"] = state_name

        return final_df
