import requests
import pandas as pd
import numpy as np
import logging
import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

filepath = os.path.split(os.getcwd())[0]
filepath = os.path.join(filepath, "myanmar_townships")
sys.path.insert(0, filepath)
from mimu_townships_data import get_mimu_townships_df


dotenv_path = Path("../.env")
load_dotenv(dotenv_path=dotenv_path)




def get_meteoblue_forecast_weather_data(lat, lon, forecast_days) -> pd.DataFrame:
    """
    Fetch weather forecast data from meteoblue Weather API.
    
    Parameters:
        lat (float): Latitude
        lon (float): Longitude
        forecast_days(int) : How many days to forecast

    Returns:
        pd.DataFrame: Weather data in DataFrame format

    """
    API_KEY = os.getenv("meteoblue_api_key")
    url = f"https://my.meteoblue.com/packages/basic-day?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json&forecast_days={forecast_days}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data due to {e}")

    meteo_weather_data =[]

    for k, v in data.items():
        # print(k, '\n', v)
        for col, val in data['data_day'].items():
            # print(col, '\n', val)
            for i in range(len(val)):
                # print(i)
                meteo_weather_data.append({
                    'date' : data['data_day']['time'][i],
                    'latitude' : data['metadata']['latitude'],
                    'longitude' : data['metadata']['latitude'],
                    'temperature_instant_°C' : data['data_day']['temperature_instant'][i],
                    'precipitation_mm' : data['data_day']['precipitation'][i],
                    'predictability_percent' : data['data_day']['predictability'][i],
                    'temperature_min_°C' : data['data_day']['temperature_min'][i],
                    'temperature_max_°C' : data['data_day']['temperature_max'][i],
                    'temperature_mean_°C' : data['data_day']['temperature_mean'][i],
                    'sealevelpressure_min_hPa' : data['data_day']['sealevelpressure_min'][i],
                    'sealevelpressure_max_hPa' : data['data_day']['sealevelpressure_max'][i],
                    'sealevelpressure_mean_hPa' : data['data_day']['sealevelpressure_mean'][i],
                    'windspeed_min_ms-1' : data['data_day']['windspeed_min'][i],
                    'windspeed_max_ms-1' : data['data_day']['windspeed_max'][i],
                    'windspeed_mean_ms-1' : data['data_day']['windspeed_mean'][i],
                    'humiditygreater90_hours_percent' : data['data_day']['humiditygreater90_hours'][i],
                    'convective_precipitation_percent' : data['data_day']['convective_precipitation'][i],
                    'relativehumidity_min_percent' : data['data_day']['relativehumidity_min'][i],
                    'relativehumidity_max_percent' : data['data_day']['relativehumidity_max'][i],
                    'relativehumidity_mean_percent' : data['data_day']['relativehumidity_mean'][i],
                    'winddirection_degree' : data['data_day']['winddirection'][i],
                    'precipitation_probability_percent' : data['data_day']['precipitation_probability'][i],
                    'uvindex' : data['data_day']['uvindex'][i],
                    'rainspot' : data['data_day']['rainspot'][i],
                    'predictability_class' : data['data_day']['predictability_class'][i],
                })

    meteo_weather_dataFrame = pd.DataFrame(meteo_weather_data)
    return meteo_weather_dataFrame


def get_MM_townships_forecast_weather():
    townships_df = get_mimu_townships_df()

    forecast_day = 7
    weather_dfs = []

    for _, row in townships_df.iterrows():
        lat = row['Latitude']
        lon = row['Longitude']
        if pd.isna(lat) or pd.isna(lon):
            continue

        try:
            weather_data = get_meteoblue_forecast_weather_data(lat, lon, forecast_day)

            # Convert weather_data to DataFrame
            if isinstance(weather_data, pd.DataFrame):
                df = weather_data.copy()
            elif isinstance(weather_data, pd.Series):
                df = weather_data.to_frame().T
            else:
                # assume dict or similar
                df = pd.DataFrame([weather_data])

            # Add lat/lon columns
            df['latitude'] = lat
            df['longitude'] = lon

            weather_dfs.append(df)
        except Exception as e:
            print(f"Failed for lat={lat}, lon={lon}: {e}")

    all_mm_townships_forecast_weather_df = pd.concat(weather_dfs, ignore_index=True)
    return all_mm_townships_forecast_weather_df


