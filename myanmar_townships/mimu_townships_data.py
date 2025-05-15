import pandas as pd
import os

def get_mimu_townships_df() -> pd.DataFrame:
    """
    Fetch Myanmar township data from Myanmar Information Management Unit Web Site.

    Returns:
        pd.DataFrame: Township data in DataFrame format
    """
    # Correct path from notebook to the file
    filepath = os.path.join("..", "myanmar_townships", "Myanmar_PCodes_Release_9.6_Feb2025_Countrywide.xlsm")
    sheet_name = "04_Town"

    # Optional: check if file really exists
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath} (working dir: {os.getcwd()})")

    townships_df = pd.read_excel(filepath, skiprows=5, sheet_name=sheet_name)
    return townships_df
