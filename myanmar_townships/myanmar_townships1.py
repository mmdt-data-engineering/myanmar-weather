import requests
import pandas as pd
import logging

def get_myanmar_townships() -> pd.DataFrame:
    """
        Fetch Myanmar geo data from a MM GEO API.
        
        Returns:
        pd.DataFrame: Myanmar townships data in DataFrame format
    """
    url = 'https://raw.githubusercontent.com/tunmin11/MM-GEO/refs/heads/master/db.json'
    
    # Step 1: Fetch data
    try:
        response = requests.get(url)
        response.raise_for_status()
        geo_data = response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")



    # Step 2: Extract townships
    townships_list = []

    for regions_list in geo_data.values():
        for region in regions_list:
            for district in region.get('districts', []):
                for township in district.get('townships', []):
                    townships_list.append({
                        'region_name_eng': region.get('eng'),
                        'region_name_mm': region.get('mm'),
                        'capital': region.get('capital'),
                        'latitude': region.get('lat'),
                        'longitude': region.get('lng'),
                        'district_name_eng': district.get('eng'),
                        'district_name_mm': district.get('mm'),
                        'township_name_eng': township.get('eng'),
                        'township_name_mm': township.get('mm')
                    })

    return pd.DataFrame(townships_list)
