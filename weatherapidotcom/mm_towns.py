import requests
import pandas as pd


def get_town_data():
    mm_geo_url = (
        "https://raw.githubusercontent.com/tunmin11/MM-GEO/refs/heads/master/db.json"
    )

    response = requests.get(mm_geo_url)
    geo_data = response.json()

    townships_data = []

    for regions_list in geo_data.values():
        for region_dict in regions_list:
            for reg_key, region in region_dict.items():
                region_name_eng = region_dict["eng"]
                region_name_mm = region_dict["mm"]
                capital = region_dict["capital"]
                latitude = region_dict["lat"]
                longitude = region_dict["lng"]
                districts = region_dict["districts"][0]
                for dist_key, district in districts.items():
                    district_name_eng = districts["eng"]
                    district_name_mm = districts["mm"]
                    # print(districts)
                    for townships in districts["townships"]:
                        # print(townships)
                        for tsp_key, township in townships.items():
                            township_name_eng = townships["eng"]
                            township_name_mm = townships["mm"]
                            # print(region_name_eng, region_name_mm, capital, latitude, longitude, district_name_eng, district_name_mm, township_name_eng, township_name_mm)
                        townships_data.append(
                            {
                                "region_name_eng": region_name_eng,
                                "region_name_mm": region_name_mm,
                                "capital": capital,
                                "latitude": latitude,
                                "longitude": longitude,
                                "district_name_eng": district_name_eng,
                                "district_name_mm": district_name_mm,
                                "township_name_eng": township_name_eng,
                                "township_name_mm": township_name_mm,
                            }
                        )
    townships_df = pd.DataFrame(townships_data)
    return townships_df
