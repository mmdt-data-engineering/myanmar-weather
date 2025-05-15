import pandas as pd
import os
import sys
import requests
import logging
from pathlib import Path


filepath = os.path.split(os.getcwd())[0]
filepath = os.path.join(filepath, "myanmar_townships")
sys.path.insert(0, filepath)
from mimu_townships_data import get_mimu_townships_df





def get_meteoblue_current_weather_data(lat, lon) -> pd.DataFrame:
    """
    Fetch weather current weather data from meteoblue Weather API.
    
    Parameters:
        lat (float): Latitude
        lon (float): Longitude

    Returns:
        pd.DataFrame: Weather data in DataFrame format

    """
    API_KEY = os.getenv("meteoblue_api_key")
    url = f"https://my.meteoblue.com/packages/basic-day_current?apikey={API_KEY}&lat={lat}&lon={lon}&asl=30&format=json"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data due to {e}")

    current_weather_list = []

    for k, v in data['data_current'].items():
        current_weather_list.append({
            "date" : pd.to_datetime(data['data_current']['time']),
            "isobserveddata" : data['data_current']['isobserveddata'],
            "metarid" : data['data_current']['metarid'],
            'isdaylight' : data['data_current']['isdaylight'],
            'windspeed' : data['data_current']['windspeed'],
            'zenithangle' : data['data_current']['zenithangle'],
            "pictocode_detailed" : data['data_current']['pictocode_detailed'],
            "pictocode" : data['data_current']['pictocode'],
            "temperature" : data['data_current']['temperature'],
        })
    
    current_weather_df = pd.DataFrame(current_weather_list)
    return current_weather_df



def get_MM_townships_current_weather():
    townships_df = get_mimu_townships_df()

    weather_dfs = []

    for _, row in townships_df.iterrows():
        lat = row['Latitude']
        lon = row['Longitude']
        if pd.isna(lat) or pd.isna(lon):
            continue

        try:
            weather_data = get_meteoblue_current_weather_data(lat, lon)
            
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

    all_mm_townships_current_weather_df = pd.concat(weather_dfs, ignore_index=True)
    return all_mm_townships_current_weather_df

