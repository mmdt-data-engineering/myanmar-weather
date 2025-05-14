import time
import pandas as pd
from dotenv import load_dotenv
import os
import requests
from data_util import MIMU_Data


class WeatherAPI:
    def __init__(self):
        pass

    def get_current(self, township_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetches current weather data for the specified townships by using latitude and longitude.
        """
        # Define the base URL for the weather API
        BASE_URL = "https://api.weatherapi.com/v1/current.json"

        # Load environment variables
        load_dotenv()
        API_KEY = os.getenv("WEATHER_API_KEY")
        # print(f"API_KEY: {API_KEY}")

        result_df = pd.DataFrame()

        for index, row in township_df.iterrows():
            township_name = row["Township_Name_Eng"]
            latitude = row["Latitude"]
            longitude = row["Longitude"]
            print(
                f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
            )

            # Construct the API request URL
            url = f"{BASE_URL}?key={API_KEY}&q={latitude},{longitude}"
            # print(url)

            # Wait for 5 seconds to avoid hitting the API rate limit
            time.sleep(10)
            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                df = pd.json_normalize(data)

                result_df = pd.concat([result_df, df], ignore_index=True)
            else:
                raise ConnectionError(f"Failed to fetch data: {response.status_code}")

        return result_df
