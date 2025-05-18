from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI
from ambientweather_api import AmbientWeatherAPI
from datetime import date
import os
from aws_utils import AmazonS3


def upload_file_to_s3(file_path: str):

    # Initialize CloudUtils
    aws_client = AmazonS3()

    try:
        aws_client.upload_file(file_path)
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")


def upload_all_output_files_to_s3():

    # Initialize CloudUtils
    aws_client = AmazonS3()

    # Set the directory containing the CSV files
    folder_path = "./output"

    if not os.path.exists(folder_path):
        raise FileNotFoundError(
            f"Folder not found: {folder_path} (working dir: {os.getcwd()})"
        )

    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    print(f"CSV files found: {csv_files}")

    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)

        try:
            aws_client.upload_file(file_path)
        except Exception as e:
            print(f"Error uploading {file_name}: {e}")
            continue
