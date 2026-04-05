CREATE TABLE IF NOT EXISTS games (
    game_id BIGINT PRIMARY KEY,
    home_team TEXT,
    away_team TEXT,
    game_date DATE,
    status TEXT
);
