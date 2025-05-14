if __name__ == "__main__":

    # Get townships from MM-GEO data
    # township_df = get_town_data()
    # print(township_df.shape)
    # print(township_df.head(5))
    # township_df.to_csv("MM_GEO_townships.csv", index=False)

    # Get townships from MIMU data
    township_df = get_townships()
    # township_df.to_csv("MIMU_townships.csv", index=False)

    township_df = township_df.head(50)

    weather_df = get_current_weather(township_df)
    # print(weather_df)

    # load_to_postgres(township_df, "townships")
    weather_df.to_csv("weatherapidotcom_data.csv", index=False, header=True)
