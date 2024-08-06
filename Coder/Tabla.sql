CREATE TABLE IF NOT EXISTS FixedCoderHouse1 (
    id INTEGER IDENTITY(1,1) PRIMARY KEY,
    currency VARCHAR(3),
    rate DOUBLE PRECISION,
    date TIMESTAMP
);
SELECT * from FixedCoderHouse1
