# -*- coding: utf-8 -*-
import requests
import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

def get_ambient_weather_data(lat, lon) -> pd.DataFrame:

    """
    Fetch weather forecast data from Ambient Weather API.

    Parameters:
        lat (float): Latitude
        lon (float): Longitude

    Returns:
        pd.DataFrame: Weather data in DataFrame format

    """

    url = f"https://lightning.ambientweather.net/forecast/{lat}/{lon}"
    headers = {
        "User-Agent": "Chrome/135.0.0.0 Safari/537.36",
        "Referer": "https://ambientweather.net",
        "Origin": "https://ambientweather.net"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")


    weather_list = []
    for dict in data['daily']['data']:
        weather_list.append({
            'date': datetime.fromtimestamp(dict['time']).strftime("%Y-%m-%d"),
            'latitude' : data['lat'],
            'longitude' : data['lon'],
            'township' : data['location']['LocalizedName'],
            'timezone' : data['tz'],
            'summary' : dict['summary'],
            'precipProbability' : dict['precipProbability'],
            'precipIntensity' :dict['precipIntensity'],
            'precipAccumulation' : dict['precipAccumulation'],
            'windSpeed' : dict['windSpeed'],
            'icon' : dict['icon'],
            'windBearing' : dict['windBearing'],
            'windGust' : dict['windGust'],
            'temperatureMin' : dict['temperatureMin'],
            'temperatureMax' : dict['temperatureMax']
        })

    weather_data_df = pd.DataFrame(weather_list)
    return weather_data_df

# GitHub raw URL of the CSV file (replace with your actual URL)
lat_lon_url = 'https://raw.githubusercontent.com/mmdt-data-engineering/myanmar-weather/refs/heads/main/lat_lon_data.csv'

# Load lat/lon data from GitHub
lat_lon_df = pd.read_csv(lat_lon_url)

def fetch_weather_data_for_all_lat_lon(lat_lon_df: pd.DataFrame) -> pd.DataFrame:
    all_weather_data = []

    # Iterate through each lat/lon pair
    for index, row in lat_lon_df.iterrows():
        lat = row['latitude']
        lon = row['longitude']

        # Fetch weather data for each lat/lon pair
        weather_data = get_ambient_weather_data(lat, lon)

        # Add lat/lon to the weather data for tracking
        weather_data['latitude'] = lat
        weather_data['longitude'] = lon

        # Append the data to the list
        all_weather_data.append(weather_data)

    # Combine all data into a single DataFrame
    final_weather_data = pd.concat(all_weather_data, ignore_index=True)
    return final_weather_data

# Call the function to fetch weather data for all lat/lon pairs
combined_weather_data = fetch_weather_data_for_all_lat_lon(lat_lon_df)

# Display the combined weather data
print(combined_weather_data.head())


# Assuming 'combined_weather_data' is the DataFrame you want to save
combined_weather_data.to_csv('combined_weather_data.csv', index=False)
