DROP TABLE IF EXISTS games;

CREATE TABLE games (
    game_id BIGINT PRIMARY KEY,
    game_date DATE,

    home_team TEXT,
    away_team TEXT,

    is_dodgers_home BOOLEAN,

    dodgers_score INT,
    opponent_score INT,

    dodgers_win BOOLEAN,

    venue TEXT,
    status TEXT
);
