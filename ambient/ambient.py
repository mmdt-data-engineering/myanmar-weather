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
    # Load environment variables
    load_dotenv()

    # Get API key and application key from environment variables
    api_key = os.getenv("AMBIENT_API_KEY")
    application_key = os.getenv("AMBIENT_APPLICATION_KEY")

    # Create an instance of the AmbientAPI class
    # ambient_api = AmbientAPI(api_key, application_key)
    ambient_api = AmbientAPI()

    # Fetch the list of devices
    devices = ambient_api.get_devices()
    print(f"Devices - {devices}")

    # Convert the list of devices to a DataFrame
    df = pd.json_normalize(devices)

    return df


def extract_sample_data():
    """
    Extracts sample data from the Ambient Weather API and returns it as a DataFrame.
    """
    # Load environment variables
    load_dotenv()

    # Get API key and application key from environment variables
    api_key = os.getenv("AMBIENT_API_KEY")
    application_key = os.getenv("AMBIENT_APPLICATION_KEY")

    # Define the URL for the Ambient Weather API
    url = f"https://api.ambientweather.net/v1/devices?apiKey={api_key}&applicationKey={application_key}"

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
    # df = extract_sample_data()
    df = get_ambient_devices()
    print(df.shape)  # (0, 0)
