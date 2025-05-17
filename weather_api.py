import pandas as pd
from dotenv import load_dotenv
import os, time, random, requests
from Logger import Logger


class WeatherAPI:
    def __init__(self):
        self.logger = Logger().get_logger("WeatherAPI")
        self.logger.info("WeatherAPI is initialized")

    def print_info(self, message):
        """
        Prints the log message to the console and logs it.
        """
        print(message)
        self.logger.info(message)

    def get_current(self, township_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fetches current weather data for the specified townships by using latitude and longitude.
        """
        # Define the base URL for the weather API
        BASE_URL = "https://api.weatherapi.com/v1/current.json"

        # Load environment variables
        load_dotenv()
        WEATHER_API_KEY = os.getenv("WEATHER_API_COM_KEY")
        if WEATHER_API_KEY is None:
            raise ValueError(
                "API key not found. Please set the WEATHER_API_KEY environment variable."
            )

        result_df = pd.DataFrame()

        for index, row in township_df.iterrows():

            township_name = row["Township_Name_Eng"]
            latitude = row["Latitude"]
            longitude = row["Longitude"]

            message = f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
            self.print_info(message)

            # Construct the API request URL
            url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={latitude},{longitude}"

            # Random sleep time between 1 and 5 seconds
            sleep_time = random.uniform(1, 5)

            message = f"Sleeping for {sleep_time:.2f} seconds..."
            self.print_info(message)

            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                df = pd.json_normalize(data)

                # remove the prefix from column names
                df.columns = df.columns.str.replace("current.", "")
                df.columns = df.columns.str.replace("location.", "")
                df.columns = df.columns.str.replace("condition.", "")

                result_df = pd.concat([result_df, df], ignore_index=True)
            else:
                raise ConnectionError(f"Failed to fetch data: {response.status_code}")

        return result_df

    def get_daily(self, township_df: pd.DataFrame, no_of_days: int) -> pd.DataFrame:
        """
        Fetches current weather data for the specified townships by using latitude and longitude.
        """
        # Define the base URL for the weather API
        BASE_URL = "https://api.weatherapi.com/v1/forecast.json"

        # Load environment variables
        load_dotenv()
        WEATHER_API_KEY = os.getenv("WEATHER_API_COM_KEY")
        if WEATHER_API_KEY is None:
            raise ValueError(
                "API key not found. Please set the WEATHER_API_KEY environment variable."
            )

        result_df = pd.DataFrame()

        for index, row in township_df.iterrows():
            township_name = row["Township_Name_Eng"]
            latitude = row["Latitude"]
            longitude = row["Longitude"]

            message = f"Township: {township_name}, Latitude: {latitude}, Longitude: {longitude}"
            self.print_info(message)

            # Construct the API request URL
            url = f"{BASE_URL}?key={WEATHER_API_KEY}&q={latitude},{longitude}&days={no_of_days}&aqi=no&alerts=no"

            sleep_time = random.uniform(1, 5)

            self.print_info(f"Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()

                # Location
                location = data["location"]
                location_df = pd.json_normalize(location)

                # Forecast
                forecast_data = data["forecast"]["forecastday"]
                df = pd.json_normalize(forecast_data)

                # Select only required columns
                forecast_df = df[
                    ["date", "date_epoch"]
                    + [col for col in df.columns if col.startswith("day.")]
                ]

                # Repeat location for each forecast row
                location_repeated = pd.concat(
                    [location_df] * len(forecast_df), ignore_index=True
                )

                # Combine location and forecast
                final_df = pd.concat([forecast_df, location_repeated], axis=1)

                # remove the "current." prefix from column names
                final_df.columns = final_df.columns.str.replace("day.", "")
                final_df.columns = final_df.columns.str.replace("condition.", "")

                result_df = pd.concat([result_df, final_df], ignore_index=True)
            else:
                raise ConnectionError(f"Failed to fetch data: {response.status_code}")

        return result_df
