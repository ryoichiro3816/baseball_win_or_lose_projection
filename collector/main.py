from kafka import KafkaProducer
import json
import requests
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 5, 0)
)

def fetch_games(date):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
    return requests.get(url).json()

def fetch_season(start_date, end_date):
    current = start_date

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        print(f"Fetching {date_str}...")

        data = fetch_games(date_str)
        producer.send("mlb-games", data)

        current += timedelta(days=1)
    producer.flush()
    print('Finish fetching')

start_date = datetime(2026, 3, 20)  # 開幕あたり
end_date = datetime.today()
fetch_season(start_date, end_date)
