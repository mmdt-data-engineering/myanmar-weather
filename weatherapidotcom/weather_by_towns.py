import time
import pandas as pd
from dotenv import load_dotenv
import os
import requests


def get_township_data(
    filepath="./data/Myanmar_PCodes_Release_9.6_Feb2025_Yangon.xlsm",
    sheet_name="04_Town",
):
    """
    Fetches township data from the specified Excel file and returns it as a DataFrame.
    """
    # Read the first worksheet
    town_data = pd.read_excel(
        filepath,
        skiprows=5,
        sheet_name=sheet_name,
    )

    # Select relevant columns
    selected_columns = ["Township_Name_Eng", "Latitude", "Longitude"]
    town_data = town_data[selected_columns]

    # Drop rows with NaN values in Latitude or Longitude
    town_data = town_data.dropna(subset=["Latitude", "Longitude"])

    return town_data


# file_path = "./data/Myanmar_PCodes_Release_9.6_Feb2025_Yangon.xlsm"
file_path = "./data/Myanmar_PCodes_Release_9.6_Feb2025_Mandalay.xlsm"
sheet_name = "04_Town"
town_lat_long = get_township_data(file_path, sheet_name)
# print(town_lat_long.head(20))

town_lat_long = town_lat_long.head(3)

for index, row in town_lat_long.iterrows():
    township_name = row["Township_Name_Eng"]
    latitude = row["Latitude"]
    longitude = row["Longitude"]
    print(f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}")

    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    print(f"API_KEY: {API_KEY}")

    BASE_URL = "https://api.weatherapi.com/v1/current.json"
    url = f"{BASE_URL}?key={API_KEY}&q={latitude},{longitude}"
    print(url)

    time.sleep(3)
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        print(df)
    else:
        raise ConnectionError(f"Failed to fetch data: {response.status_code}")
