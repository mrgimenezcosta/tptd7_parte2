

CREATE TABLE IF NOT EXISTS people (
    first_name VARCHAR(51),
    last_name VARCHAR(52),
    phone_number VARCHAR(53),
    address VARCHAR(100),
    country VARCHAR(54),
    date_of_birth TIMESTAMP,
    passport_number VARCHAR(55) PRIMARY KEY,
    email VARCHAR(56)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    event_time TIMESTAMP,
    user_agent VARCHAR(300),
    person_passport_number VARCHAR(55) REFERENCES people
);