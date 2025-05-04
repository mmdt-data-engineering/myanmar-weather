from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
import time

# Launch Chrome browser
driver = webdriver.Chrome()

# Open the weather page
driver.get("https://www.mmweather.org/divisional-cities-weather/ayarwaddy-div")
time.sleep(3)

#git CSV
with open("weather_forecast_all_cities.csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "City", "Location", "Day", "Date", "Wind Speed", "Wind Gust", "UV", "Humidity",
        "Precipitation", "Precipitation Probability", "Pressure", "Forecast Link"
    ])

    # Loop (one per city)
    top_iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"🌐 Total city weather widgets found: {len(top_iframes)}")

    for index, outer_frame in enumerate(top_iframes):
        print(f"\n🔄 Processing city {index + 1}/{len(top_iframes)}...")

        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(outer_frame)  # Level 1
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))

            inner_frame = driver.find_elements(By.TAG_NAME, "iframe")[0]
            driver.switch_to.frame(inner_frame)  # Level 2

            user_html_frame = driver.find_elements(By.TAG_NAME, "iframe")[0]
            driver.switch_to.frame(user_html_frame)  # Level 3

            
            city_name = driver.find_element(By.CSS_SELECTOR, "div.header h1").text.strip()
            location_desc = driver.find_element(By.CSS_SELECTOR, "div.location-description").text.strip()

            a_tags = driver.find_elements(By.CSS_SELECTOR, "div.picto a")

            for a in a_tags:
                def get(css_selector):
                    try:
                        return a.find_element(By.CSS_SELECTOR, css_selector).text.strip()
                    except:
                        return ""

                writer.writerow([
                    city_name,
                    location_desc,
                    get("div.day-short"),
                    get("div.day-long"),
                    get("div.wind.speed"),
                    get("div.wind.gust"),
                    get("div.coloured.uv"),
                    get("div.humidity"),
                    get("div.coloured.precipitation"),
                    get("div.coloured.precipitation.probability"),
                    get("div.pressure"),
                    a.get_attribute("href")
                ])

        except Exception as e:
            print(f"⚠️ Skipped city {index + 1} due to error: {e}")
            continue

        driver.switch_to.default_content()

driver.quit()
print("\nAll cities scraped. Data saved to 'weather_forecast_all_cities.csv'")
