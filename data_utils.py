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

        township_df.drop_duplicates(
            subset=["Township_Name_Eng"], inplace=True, keep="first"
        )

        # Drop rows with NaN values in Latitude or Longitude
        township_df = township_df.dropna(subset=["Latitude", "Longitude"])

        return township_df
