import aiohttp
import asyncio
from Logger import Logger
import time

max_retries = 3
retry_delay = 3  # Seconds

def print_error(message: str):
    print(message)
    logger = Logger().get_logger("Async Fetch")
    logger.error(message)

async def fetch(url, headers=None, params=None):
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                if headers != None and params != None:
                    async with session.get(url, headers=headers, params=params) as response:
                        return await response.text()
                elif headers != None: 
                    async with session.get(url, headers=headers) as response:
                        return await response.text()
                else:
                    async with session.get(url) as response:
                        return await response.text()
        except aiohttp.ClientError as e:  # Catch general aiohttp errors
            print_error(f"Request error: {e}")
            print_error(f"Attempt {attempt+1} failed: {e}")
            time.sleep(retry_delay)
        except asyncio.TimeoutError:  # Catch timeout errors
            print_error("Request timed out")
            print_error(f"Attempt {attempt+1} failed: {e}")
            time.sleep(retry_delay)
        except Exception as e:  # Catch unexpected errors
            print_error(f"Unexpected error: {e}")
            print_error(f"Attempt {attempt+1} failed: {e}")
            time.sleep(retry_delay)
    else:
        print_error("All retries failed.")
