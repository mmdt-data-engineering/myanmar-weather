import pandas as pd


class MIMU_Data:
    def __init__(self):
        pass

    def get_townships(
        self,
        filepath="./data/Myanmar_PCodes_Release_9.6_Feb2025_Countrywide.xlsm",
        sheet_name="04_Town",
    ):
        """
        Fetches township data from the specified Excel file and returns it as a DataFrame.
        """
        # Read the first worksheet
        township_df = pd.read_excel(
            filepath,
            skiprows=5,
            sheet_name=sheet_name,
        )

        # We are not able to use sheet 3 [03_Township] because there is no latitude, longitude columns in sheet.
        # we will continue using the sheet [04_Town] by drop duplicates of township names.

        # 535 rows in total - towns
        # 351 rows after drop duplicates - township

        township_df.drop_duplicates(subset=["Tsp_Pcode"], inplace=True, keep="first")

        # Drop rows with NaN values in Latitude or Longitude
        township_df = township_df.dropna(subset=["Latitude", "Longitude"])

        return township_df


def get_weather_description(weather_code: int) -> str:
    """
    Returns the weather description based on the weather code.
    """
    weather_codes = {
        0: "Clear sky",
        1: "Mostly clear",
        2: "Partly cloudy",
        3: "Cloudy",
        45: "Fog",
        48: "Freezing fog",
        51: "Light drizzle",
        53: "Drizzle",
        55: "Heavy drizzle",
        61: "Light rain",
        63: "Rain",
        65: "Heavy rain",
        80: "Light rain shower",
        81: "Rain shower",
        82: "Heavy rain shower",
        95: "Thunderstorm",
        96: "Thunderstorm with heavy rain",
        99: "Severe thunderstorm",
    }

    return weather_codes.get(weather_code, "Unknown weather code")
