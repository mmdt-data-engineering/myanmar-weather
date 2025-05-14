from data_utils import MIMU_Data
from weather_api import WeatherAPI
from openmeteo_api import OpenMeteoAPI

if __name__ == "__main__":

    # Get townships from MIMU data
    mimu_data = MIMU_Data()
    township_df = mimu_data.get_townships()
    # township_df.to_csv("MIMU_townships.csv", index=False)

    township_df = township_df.head(1)

    # weather_api = WeatherAPI()
    # weatherapi_current_df = weather_api.get_current(township_df)
    # print(weatherapi_current_df)

    # # load_to_postgres(township_df, "townships")
    # weatherapi_current_df.to_csv(
    #     "./output/weatherapi_current.csv", index=False, header=True
    # )

    # weatherapi_daily_df = weather_api.get_daily(township_df, no_of_days=7)
    # print(weatherapi_daily_df)
    # weatherapi_daily_df.to_csv(
    #     "./output/weatherapi_daily.csv", index=False, header=True
    # )

    openmeteo_api = OpenMeteoAPI()

    # openmeteo_current_df = openmeteo_api.get_current(township_df)
    # openmeteo_current_df.to_csv(
    #     "./output/open_meteo_current.csv", index=False, header=True
    # )

    openmeteo_daily_df = openmeteo_api.get_daily(township_df)
    print(openmeteo_daily_df)
    # openmeteo_daily_df.to_csv("./output/open_meteo_daily.csv", index=False, header=True)
