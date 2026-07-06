CREATE TABLE rates(
    rate_date DATE,
    char_code VARCHAR(3),
    nominal INT,
    value NUMERIC(10,4),
    rate_per_unit NUMERIC(10,4),
    loaded_at TIMESTAMP,
    PRIMARY KEY (rate_date, char_code),
    FOREIGN KEY (char_code) REFERENCES currencies(char_code)
);