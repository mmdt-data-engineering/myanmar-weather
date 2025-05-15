from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI
from ambientweather_api import AmbientWeatherAPI


if __name__ == "__main__":

    # Get townships from MIMU data
    mimu_data = MIMU_Data()
    township_df = mimu_data.get_townships()

    township_df = township_df.head(1)

    weather_api = WeatherAPI()

    # weatherapi - current
    weatherapi_current_df = weather_api.get_current(township_df)
    filename = "./output/weatherapi_current.csv"
    weatherapi_current_df.to_csv(filename, index=False, header=True)

    # weatherapi - daily
    weatherapi_daily_df = weather_api.get_daily(township_df, no_of_days=7)
    filename = "./output/weatherapi_daily.csv"
    weatherapi_daily_df.to_csv(filename, index=False, header=True)

    openmeteo_api = OpenMeteoAPI()

    # open-meteo - current
    openmeteo_current_df = openmeteo_api.get_current(township_df)
    filename = "./output/open_meteo_current.csv"
    openmeteo_current_df.to_csv(filename, index=False, header=True)

    # open-meteo - daily
    openmeteo_daily_df = openmeteo_api.get_daily(township_df)
    filename = "./output/open_meteo_daily.csv"
    openmeteo_daily_df.to_csv(filename, index=False, header=True)

    # Fetch AmbientWeather data
    ambient_api = AmbientWeatherAPI()  # New instance for AmbientWeatherAPI
    ambient_df = ambient_api.get_forecast_df(township_df)  # Get forecast data
    ambient_df.to_csv("./output/ambientweather_data.csv", index=False, header=True)
