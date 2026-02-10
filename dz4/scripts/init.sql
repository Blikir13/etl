CREATE TABLE IF NOT EXISTS weather_cleaned (
    id TEXT PRIMARY KEY,
    room_id TEXT,
    noted_date DATE,
    temp NUMERIC,
    out_in TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

