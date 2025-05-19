import aiohttp
import asyncio
from Logger import Logger


def print_error(message: str):
    print(message)
    logger = Logger().get_logger("Async Fetch")
    logger.error(message)

async def fetch(url, headers=None):
    try:
        async with aiohttp.ClientSession() as session:
            if headers == None:
                async with session.get(url) as response:
                    return await response.text()
            else: 
                async with session.get(url, headers=headers) as response:
                    return await response.text()
    except aiohttp.ClientError as e:  # Catch general aiohttp errors
        print_error(f"Request error: {e}")
    except asyncio.TimeoutError:  # Catch timeout errors
        print_error("Request timed out")
    except Exception as e:  # Catch unexpected errors
        print_error(f"Unexpected error: {e}")
