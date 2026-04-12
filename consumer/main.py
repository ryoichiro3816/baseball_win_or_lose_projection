import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "mlb-games",
    bootstrap_servers=['kafka:9092'],
    group_id='mlb-ml-group',
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

def save_games_batch(conn, game_list):
    if not game_list:
        return

    cur = conn.cursor()

    data_to_insert = [
        (
            g['game_id'], g['game_date'], g['home_team'], g['away_team'],
            g['is_dodgers_home'], g['dodgers_score'], g['opponent_score'],
            g['dodgers_win'], g['venue'], g['status']
        )
        for g in game_list
    ]

    sql = """
        INSERT INTO games (
            game_id, game_date, home_team, away_team,
            is_dodgers_home, dodgers_score, opponent_score,
            dodgers_win, venue, status
        )
        VALUES %s
        ON CONFLICT (game_id) DO UPDATE SET
            dodgers_score = EXCLUDED.dodgers_score,
            opponent_score = EXCLUDED.opponent_score,
            dodgers_win = EXCLUDED.dodgers_win,
            status = EXCLUDED.status
    """

    try:
        execute_values(cur, sql, data_to_insert)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"DB Error: {e}")
    finally:
        cur.close()

conn = psycopg2.connect(**DB_CONFIG)

BATCH_SIZE = 50  # 50件溜まったら書き込む
buffer = []

try:
    for msg in consumer:
        data = msg.value

        for date in data.get("dates", []):
            for game in date.get("games", []):
                home = game["teams"]["home"]["team"]["name"]
                away = game["teams"]["away"]["team"]["name"]

                if DODGERS_NAME not in home and DODGERS_NAME not in away:
                    continue
                
                # 整形してバッファに追加
                is_home = (home == DODGERS_NAME)

                # スコア（試合前はNoneになる）
                dodgers_score = None
                opponent_score = None
                dodgers_win = None

                if game["status"]["detailedState"] == "Final":
                    home_score = game["teams"]["home"]["score"]
                    away_score = game["teams"]["away"]["score"]

                if is_home:
                    dodgers_score = home_score
                    opponent_score = away_score
                else:
                    dodgers_score = away_score
                    opponent_score = home_score
                    
                dodgers_win = dodgers_score > opponent_score
                game_info = {
                    "game_id": game["gamePk"],
                    "game_date": date["date"],
                    "home_team": home,
                    "away_team": away,
                    "is_dodgers_home": is_home,
                    "dodgers_score": home_score if is_home else away_score,
                    "opponent_score": away_score if is_home else home_score,
                    "dodgers_win": dodgers_win,
                    "venue": game["venue"]["name"],
                    "status": game["status"]["detailedState"]
                }
                buffer.append(game_info)

        if len(buffer) >= BATCH_SIZE:
            save_games_batch(conn, buffer)
            buffer = []  # バッファを空にする

except KeyboardInterrupt:
    if buffer:
        save_games_batch(conn, buffer)
finally:
    conn.close()
