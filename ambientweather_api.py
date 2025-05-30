import pandas as pd
import random
import time
from datetime import datetime, date
from Logger import Logger
from fetch_data import fetch
import json


class AmbientWeatherAPI:
    def __init__(self):
        self.base_url = "https://lightning.ambientweather.net/forecast"
        self.headers = {
            "User-Agent": "Chrome/135.0.0.0 Safari/537.36",
            "Referer": "https://ambientweather.net",
            "Origin": "https://ambientweather.net",
        }
        self.logger = Logger().get_logger("AmbientWeatherAPI")
        self.print_info("AmbientWeatherAPI is initialized")

    def print_info(self, message):
        """
        Prints the log message to the console and logs it.
        """
        print(message)
        self.logger.info(message)

    async def get_forecast_df(self, township_df: pd.DataFrame) -> pd.DataFrame:
        all_data = []

        # Get today's date
        today = date.today()
        str_today = today.strftime("%Y-%m-%d")

        for _, row in township_df.iterrows():

            tsp_pcode = row["Tsp_Pcode"]
            township_name = row["Township_Name_Eng"]
            lat = row["Latitude"]
            lon = row["Longitude"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                message = f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}"
                self.print_info
                continue

            message = f"Fetching weather data for Township: {township_name}, Latitude: {lat}, Longitude: {lon}"
            self.print_info(message)

            url = f"{self.base_url}/{lat}/{lon}"

            # Sleep between 1 to 5 seconds
            # time.sleep(random.uniform(1, 5))

            # response = requests.get(url, headers=self.headers)
            response, status = await fetch(url, headers=self.headers)

            if status != 200:
                raise ConnectionError(f"Fetch data from Ambient Weather API - FAILED ")

            data = json.loads(response)

            weather_list = []
            for item in data["daily"]["data"]:
                weather_list.append(
                    {
                        "data_source": "ambient weather",
                        "date": datetime.fromtimestamp(item["time"]).strftime(
                            "%Y-%m-%d"
                        ),
                        "extraction_date": str_today,  # Add today's date as extraction date
                        "tsp_pcode": tsp_pcode,
                        "township": township_name,
                        "latitude": data.get("lat", lat),
                        "longitude": data.get("lon", lon),
                        "timezone": data.get("tz", None),
                        "summary": item.get("summary", None),
                        "precipitation_probability": item.get(
                            "precipProbability", None
                        ),
                        "precipitation_intensity": item.get("precipIntensity", None),
                        "precipitation_intensity_unit": "in/hr",
                        "precipitation_accumulation": item.get(
                            "precipAccumulation", None
                        ),
                        "precipitation_accumulation_unit": "in",  # <-- add unit
                        "wind_speed": item.get("windSpeed", None),
                        "wind_speed_unit": "mph",  # <-- add unit
                        "icon": item.get("icon", None),
                        "wind_bearing": item.get("windBearing", None),
                        "wind_bearing_unit": "degrees",  # <-- add unit
                        "wind_gust": item.get("windGust", None),
                        "wind_gust_unit": "mph",  # <-- add unit
                        "temperature_min": item.get("temperatureMin", None),
                        "temperature_min_unit": "F",  # <-- add unit
                        "temperature_max": item.get("temperatureMax", None),
                        "temperature_max_unit": "F",  # <-- add unit
                    }
                )

            daily_df = pd.DataFrame(weather_list)
            all_data.append(daily_df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
