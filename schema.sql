CREATE TABLE IF NOT EXISTS user_logs (
    timestamp TIMESTAMP,
    user_id INT,
    action TEXT,
    value FLOAT
);