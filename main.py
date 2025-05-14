from data_utils import MIMU_Data
from weather_api import WeatherAPI

if __name__ == "__main__":

    # Get townships from MIMU data
    township = MIMU_Data()
    township_df = township.get_townships()
    # township_df.to_csv("MIMU_townships.csv", index=False)

    township_df = township_df.head(1)

    weather_api = WeatherAPI()
    weather_df = weather_api.get_current(township_df)
    print(weather_df)

    # load_to_postgres(township_df, "townships")
    weather_df.to_csv("./output/weatherapidotcom_data.csv", index=False, header=True)
