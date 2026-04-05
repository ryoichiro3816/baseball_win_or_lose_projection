import requests
import psycopg2

DB_CONFIG = {
    "host": "db",
    "dbname": "mlb",
    "user": "user",
    "password": "password"
}

def fetch_games(date):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
    return requests.get(url).json()

def save_games(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for date in data.get("dates", []):
        for game in date.get("games", []):
            game_id = game["gamePk"]
            home = game["teams"]["home"]["team"]["name"]
            away = game["teams"]["away"]["team"]["name"]

            cur.execute("""
                INSERT INTO games (game_id, home_team, away_team)
                VALUES (%s, %s, %s)
                ON CONFLICT (game_id) DO NOTHING
            """, (game_id, home, away))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    data = fetch_games("2026-04-05")
    save_games(data)
