import psycopg2
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "mlb-games",
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    api_version=(2, 5, 0)
)

DB_CONFIG = {
    "host": "db",
    "dbname": "mlb",
    "user": "user",
    "password": "password"
}

DODGERS_NAME = "Los Angeles Dodgers"

def save_games(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for date in data.get("dates", []):
        for game in date.get("games", []):

            home = game["teams"]["home"]["team"]["name"]
            away = game["teams"]["away"]["team"]["name"]

            # 👉 Dodgers以外はスキップ
            if DODGERS_NAME not in home and DODGERS_NAME not in away:
                continue

            game_id = game["gamePk"]
            game_date = date["date"]

            is_dodgers_home = (home == DODGERS_NAME)

            # スコア（試合前はNoneになる）
            dodgers_score = None
            opponent_score = None
            dodgers_win = None

            if game["status"]["detailedState"] == "Final":
                home_score = game["teams"]["home"]["score"]
                away_score = game["teams"]["away"]["score"]

                if is_dodgers_home:
                    dodgers_score = home_score
                    opponent_score = away_score
                else:
                    dodgers_score = away_score
                    opponent_score = home_score

                dodgers_win = dodgers_score > opponent_score

            cur.execute("""
                INSERT INTO games (
                    game_id,
                    game_date,
                    home_team,
                    away_team,
                    is_dodgers_home,
                    dodgers_score,
                    opponent_score,
                    dodgers_win,
                    venue,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    dodgers_score = EXCLUDED.dodgers_score,
                    opponent_score = EXCLUDED.opponent_score,
                    dodgers_win = EXCLUDED.dodgers_win,
                    status = EXCLUDED.status
            """, (
                game_id,
                game_date,
                home,
                away,
                is_dodgers_home,
                dodgers_score,
                opponent_score,
                dodgers_win,
                game["venue"]["name"],
                game["status"]["detailedState"]
            ))

    conn.commit()
    cur.close()
    conn.close()

for msg in consumer:
    data = msg.value
    save_games(data)