import requests
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from time import sleep
import influx_config
import logging
import os

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=log_level,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

bucket = influx_config.bucket
org = influx_config.org
token = influx_config.INFLUX_TOKEN
url = influx_config.url
openWeatherMap_token = influx_config.OPEN_WEATHER_MAP_TOKEN
SLEEP_PERIOD = 60


# Get time series data from OpenWeatherMap API
openWeather_url = "https://api.openweathermap.org/data/2.5/weather"
params = {
    'q': 'Seattle',
    'appid': openWeatherMap_token,
    'units': 'imperial'
}


while True:
    try:
        sleep(60)
        r = requests.get(openWeather_url, params=params)
        if r.status_code != 200:
            logging.error(f"Error fetching weather data: {weather_dict.get('message', 'Unknown error')}")
            sleep(SLEEP_PERIOD)
            continue 

        weather_dict = dict(r.json())   

        current_temp = float(weather_dict['main']['temp'])
        humidity = int(weather_dict['main']['humidity'])
        pressure = int(weather_dict['main']['pressure'])

        logging.info(f'Current temp: {current_temp}')
        logging.info(f'Current humidity: {humidity}')
        logging.info(f'Current pressure: {pressure}')

        write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        write_api = write_client.write_api(write_options=SYNCHRONOUS) 

        point = (
        Point("weather")
        .tag("location", "internet")
        .field("outdoor_temp", current_temp)
        )
        write_api.write(bucket=bucket, org="ORG_NAME", record=point)

        point = (
        Point("weather")
        .tag("location", "internet")
        .field("outdoor_humidity", humidity)
        )
        write_api.write(bucket=bucket, org="ORG_NAME", record=point)

        point = (
        Point("weather")
        .tag("location", "internet")
        .field("outdoor_pressure", pressure)
        )
        write_api.write(bucket=bucket, org="ORG_NAME", record=point)


    except Exception as e:
        logging.error(e)