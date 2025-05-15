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

        # Select relevant columns
        # selected_columns = ["Township_Name_Eng", "Latitude", "Longitude"]
        # township_df = township_df[selected_columns]

        # Drop rows with NaN values in Latitude or Longitude
        township_df = township_df.dropna(subset=["Latitude", "Longitude"])

        return township_df
