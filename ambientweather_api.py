import pandas as pd
import random
import time
from datetime import datetime, date
from Logger import Logger
from fetch_data import fetch_with_headers
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
            township_name = row["Township_Name_Eng"]
            lat = row["Latitude"]
            lon = row["Longitude"]
            town_name = row["Town_Name_Eng"]
            district_name = row["District/SAZ_Name_Eng"]
            state_name = row["SR_Name_Eng"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                message = f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}"
                self.print_info
                continue

            message = f"Fetching weather data for Township: {township_name}, Latitude: {lat}, Longitude: {lon}"
            self.print_info(message)

            url = f"{self.base_url}/{lat}/{lon}"
            time.sleep(random.uniform(1, 5))  # Sleep between 1 to 5 seconds

            # response = requests.get(url, headers=self.headers)
            response = await fetch_with_headers(url, headers=self.headers)
            # response.raise_for_status()

            data = json.loads(response)

            weather_list = []
            for item in data["daily"]["data"]:
                weather_list.append(
                    {
                        "date": datetime.fromtimestamp(item["time"]).strftime(
                            "%Y-%m-%d"
                        ),
                        "latitude": data.get("lat", lat),
                        "longitude": data.get("lon", lon),
                        "township": township_name,
                        "town name": town_name,
                        "district name": district_name,
                        "state name": state_name,
                        "timezone": data.get("tz", None),
                        "summary": item.get("summary", None),
                        "precipProbability": item.get("precipProbability", None),
                        "precipIntensity": item.get("precipIntensity", None),
                        "precipAccumulation": item.get("precipAccumulation", None),
                        "windSpeed": item.get("windSpeed", None),
                        "icon": item.get("icon", None),
                        "windBearing": item.get("windBearing", None),
                        "windGust": item.get("windGust", None),
                        "temperatureMin": item.get("temperatureMin", None),
                        "temperatureMax": item.get("temperatureMax", None),
                        "extraction_date": str_today,  # Add today's date as extraction date
                    }
                )

            daily_df = pd.DataFrame(weather_list)
            all_data.append(daily_df)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
