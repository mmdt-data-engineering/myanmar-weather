import requests
import pandas as pd
import numpy as np
import os
import logging
from sqlalchemy import create_engine
import platform
from dotenv import load_dotenv
from ambient_api.ambientapi import AmbientAPI


def get_ambient_devices():
    """
    Fetches the list of devices from the Ambient Weather API and returns it as a DataFrame.
    """

    # https://rt.ambientweather.net/v1/devices/?apiKey=%7B%7Bapi_key%7D%7D&applicationKey=%7B%7Bapplication_key%7D%7D

    # Create an instance of the AmbientAPI class
    print(f"AMBIENT_ENDPOINT: {AMBIENT_ENDPOINT}")
    print(f"AMBIENT_API_KEY: {AMBIENT_API_KEY}")
    print(f"AMBIENT_APPLICATION_KEY: {AMBIENT_APPLICATION_KEY}")

    url = f"{AMBIENT_ENDPOINT}/devices?apiKey={AMBIENT_API_KEY}&applicationKey={AMBIENT_APPLICATION_KEY}"
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        raise ConnectionError(f"Failed to fetch data: {response.status_code}")


def extract_sample_data():
    """
    Extracts sample data from the Ambient Weather API and returns it as a DataFrame.
    """

    # Define the URL for the Ambient Weather API
    # url = f"https://api.ambientweather.net/v1/devices?apiKey={AMBIENT_API_KEY}&applicationKey={AMBIENT_APPLICATION_KEY}"
    url = f"https://rt.ambientweather.net/v1/devices?apiKey={AMBIENT_API_KEY}&applicationKey={AMBIENT_APPLICATION_KEY}"

    # Make a GET request to the API
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        raise ConnectionError(f"Failed to fetch data: {response.status_code}")


if __name__ == "__main__":

    # Load environment variables
    load_dotenv()

    AMBIENT_ENDPOINT = os.getenv("AMBIENT_ENDPOINT")
    AMBIENT_API_KEY = os.getenv("AMBIENT_API_KEY")
    AMBIENT_APPLICATION_KEY = os.getenv("AMBIENT_APPLICATION_KEY")

    df = get_ambient_devices()
    print(df.shape)  # (0, 0)

    df = extract_sample_data()
    print(df.shape)  # (0, 0)
