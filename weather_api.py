import pandas as pd
from dotenv import load_dotenv
import os, time, random
from Logger import Logger
from fetch_data import fetch
import json


class WeatherAPI:
    def __init__(self):
        self.logger = Logger().get_logger("WeatherAPI")
        self.print_info("WeatherAPI is initialized")

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
        tsp_pcode = row["Tsp_Pcode"]
        township_name = row["Township_Name_Eng"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]

        message = (
            f"TownName: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
        )
        self.print_info(message)

        # Construct the API request URL
        url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={latitude},{longitude}"

        # Random sleep time between 1 and 5 seconds
        # sleep_time = random.uniform(1, 5)
        # self.print_info(f"Sleeping for {sleep_time:.2f} seconds...")
        # time.sleep(sleep_time)

        # response = requests.get(url)
        response, status = await fetch(url)

        if status != 200:
            raise ConnectionError(f"Fetch data from Weather API - FAILED ")

        data = json.loads(response)

        location = data["location"]
        current = data["current"]
        condition = data["current"]["condition"]

        current_list = []
        current_list.append(
            {
                "source": "weatherapi",
                "date": current.get("last_updated", None),
                "extraction_date": pd.to_datetime(time.strftime("%Y-%m-%d")),
                "tsp_code": tsp_pcode,
                "township": township_name,
                "latitude": location.get("lat", None),
                "longitude": location.get("lon", None),
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
        tsp_pcode = row["Tsp_Pcode"]
        township_name = row["Township_Name_Eng"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        town_name = row["Town_Name_Eng"]

        message = f"Town Name:{town_name}, Latitude:{latitude}, Longitude:{longitude}"
        self.print_info(message)

        # Construct the API request URL
        url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={latitude},{longitude}&days={NO_OF_DAYS}&aqi=no&alerts=no"

        # Random sleep time between 1 and 5 seconds
        # sleep_time = random.uniform(1, 5)
        # self.print_info(f"Sleeping for {sleep_time:.2f} seconds...")
        # time.sleep(sleep_time)

        # response = requests.get(url)
        response, status = await fetch(url)

        if status != 200:
            raise ConnectionError(f"Fetch data from Weather API - FAILED ")

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

        print(final_df.columns)
        print(final_df.head(5))

        units = {
            "temp_c": "celsius",
            "temp_f": "farenheit",
            "wind_kph": "kph",
            "wind_mph": "kph",
            "precip_mm": "millimeters",
            "precip_in": "inches",
            "snow_cm": "centimeters",
            "vis_km": "kilometers",
            "vis_miles": "miles",
        }

        final_df["tsp_code"] = tsp_pcode
        final_df["township_name"] = township_name
        final_df["extraction_date"] = pd.to_datetime(time.strftime("%Y-%m-%d"))

        # Build final list of dicts
        weather_data = []
        for _, row in final_df.iterrows():
            weather_data.append(
                {
                    "source": "weatherapi",
                    "date": row["date"],
                    "extraction_date": row["extraction_date"],
                    "tsp_code": tsp_pcode,
                    "township": township_name,
                    "latitude": row["lat"],
                    "longitude": row["lon"],
                    "date_epoch": row["date_epoch"],
                    "temperature_min_c": row["mintemp_c"],
                    "temperature_min_c_unit": units["temp_c"],
                    "temperature_max_c": row["maxtemp_c"],
                    "temperature_max_c_unit": units["temp_c"],
                    "temperature_avg_c": row["avgtemp_c"],
                    "temperature_avg_c_unit": units["temp_c"],
                    "temperature_min_f": row["mintemp_f"],
                    "temperature_min_f_unit": units["temp_f"],
                    "temperature_max_f": row["maxtemp_f"],
                    "temperature_max_f_unit": units["temp_f"],
                    "temperature_avg_f": row["avgtemp_f"],
                    "temperature_avg_f_unit": units["temp_f"],
                    "wind_max_kph": row["maxwind_kph"],
                    "wind_max_kph_unit": units["wind_kph"],
                    "wind_max_mph": row["maxwind_mph"],
                    "wind_max_mph_unit": units["wind_mph"],
                    "precipitation_total_mm": row["totalprecip_mm"],
                    "precipitation_total_mm_unit": units["precip_mm"],
                    "precipitation_total_in": row["totalprecip_in"],
                    "precipitation_total_in_unit": units["precip_in"],
                    "snow_total_cm": row["totalsnow_cm"],
                    "snow_total_cm_unit": units["snow_cm"],
                    "visibility_avg_km": row["avgvis_km"],
                    "visibility_avg_km_unit": units["vis_km"],
                    "visibility_avg_miles": row["avgvis_miles"],
                    "visibility_avg_miles_unit": units["vis_miles"],
                    "humidity_avg": row["avghumidity"],
                    "uv_index": row["uv"],
                    "condition_text": row["text"],
                    "condition_icon": row["icon"],
                    "condition_code": row["code"],
                }
            )
        weather_df = pd.DataFrame(weather_data)
        print(weather_df.columns)
        print(weather_df.head(5))

        return weather_df
