import pandas as pd
from openmeteo_api import get_hourly_forecast_df

if __name__ == "__main__":

    # Get townships from MIMU data
    township_df = pd.read_csv("../weatherapidotcom/MIMU_townships.csv")

    township_df = township_df[["Township_Name_Eng", "Latitude", "Longitude"]]
    township_df = township_df.head(3)
    # print(township_df)

    weather_df = get_hourly_forecast_df(township_df)

    print(weather_df)

    # load_to_postgres(township_df, "townships")
    weather_df.to_csv("open-meteo_hourly.csv", index=False, header=True)
