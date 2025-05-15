import requests
import pandas as pd
import random
import time
from datetime import datetime

class AmbientWeatherAPI:
    def __init__(self):
        self.base_url = "https://lightning.ambientweather.net/forecast"
        self.headers = {
            "User-Agent": "Chrome/135.0.0.0 Safari/537.36",
            "Referer": "https://ambientweather.net",
            "Origin": "https://ambientweather.net"
        }

    def get_forecast_df(self, township_df: pd.DataFrame) -> pd.DataFrame:
        all_data = []

        for _, row in township_df.iterrows():
            township_name = row["Township_Name_Eng"]
            lat = row["Latitude"]
            lon = row["Longitude"]

            # ✅ Skip rows with missing values
            if pd.isnull(lat) or pd.isnull(lon) or pd.isnull(township_name):
                print(f"[SKIP] Missing data for township: {township_name}, lat: {lat}, lon: {lon}")
                continue

            print(f"Fetching weather data for Township: {township_name}, Latitude: {lat}, Longitude: {lon}")

            url = f"{self.base_url}/{lat}/{lon}"
            time.sleep(random.uniform(1, 5))  # Sleep between 1 to 5 seconds

            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()

                weather_list = []
                for item in data['daily']['data']:
                    weather_list.append({
                        'date': datetime.fromtimestamp(item['time']).strftime("%Y-%m-%d"),
                        'latitude': data.get('lat', lat),
                        'longitude': data.get('lon', lon),
                        'township': township_name,
                        'timezone': data.get('tz', None),
                        'summary': item.get('summary', None),
                        'precipProbability': item.get('precipProbability', None),
                        'precipIntensity': item.get('precipIntensity', None),
                        'precipAccumulation': item.get('precipAccumulation', None),
                        'windSpeed': item.get('windSpeed', None),
                        'icon': item.get('icon', None),
                        'windBearing': item.get('windBearing', None),
                        'windGust': item.get('windGust', None),
                        'temperatureMin': item.get('temperatureMin', None),
                        'temperatureMax': item.get('temperatureMax', None)
                    })

                daily_df = pd.DataFrame(weather_list)
                all_data.append(daily_df)

            except requests.RequestException as e:
                print(f"[ERROR] Failed to fetch data for ({lat}, {lon}): {e}")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
