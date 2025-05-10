import time
import pandas as pd
from dotenv import load_dotenv
import os
import requests
from mimu_data import get_townships
from database import load_to_postgres
from mm_towns import get_town_data


def get_current_weather(df: pd.DataFrame) -> None:
    """
    Fetches current weather data for the specified townships by using latitude and longitude.
    """
    # Define the base URL for the weather API
    BASE_URL = "https://api.weatherapi.com/v1/current.json"

    result_df = pd.DataFrame()

    # Iterate over each township in the DataFrame
    for index, row in township_df.iterrows():
        township_name = row["Township_Name_Eng"]
        latitude = row["Latitude"]
        longitude = row["Longitude"]
        print(
            f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
        )

        # Load environment variables
        load_dotenv()
        API_KEY = os.getenv("API_KEY")
        # print(f"API_KEY: {API_KEY}")

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


if __name__ == "__main__":

    # Get townships from MM-GEO data
    # township_df = get_town_data()
    # print(township_df.shape)
    # print(township_df.head(5))
    # township_df.to_csv("MM_GEO_townships.csv", index=False)

    # Get townships from MIMU data
    township_df = get_townships()
    # township_df.to_csv("MIMU_townships.csv", index=False)

    township_df = township_df.head(50)

    weather_df = get_current_weather(township_df)
    # print(weather_df)

    # load_to_postgres(township_df, "townships")
    weather_df.to_csv("weatherapidotcom_data.csv", index=False, header=True)
