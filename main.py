from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI

if __name__ == "__main__":

    # Get townships from MIMU data
    mimu_data = MIMU_Data()
    township_df = mimu_data.get_townships()
    # township_df.to_csv("MIMU_townships.csv", index=False)

    township_df = township_df.head(1)

    weather_api = WeatherAPI()
    weather_df = weather_api.get_current(township_df)
    print(weather_df)

    # load_to_postgres(township_df, "townships")
    # weather_df.to_csv("./output/weatherapidotcom_data.csv", index=False, header=True)

    openmeteo_api = OpenMeteoAPI()
    openmeteo_df = openmeteo_api.get_hourly_forecast_df(township_df)
    openmeteo_df.to_csv("./output/open_meteo_data.csv", index=False, header=True)
