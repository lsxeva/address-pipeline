-- schema.sql

-- Transactions table
CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    house VARCHAR(50),
    street VARCHAR(100),
    strtype VARCHAR(50),
    apttype VARCHAR(50),
    aptnbr VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(2),
    zip VARCHAR(10),
    original_address TEXT,
    normalized_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Canonical addresses table
CREATE TABLE canonical_addresses (
    hhid VARCHAR(50) PRIMARY KEY,
    house VARCHAR(50),
    street VARCHAR(100),
    strtype VARCHAR(50),
    apttype VARCHAR(50),
    aptnbr VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(2),
    zip VARCHAR(10),
    normalized_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Matching results table
CREATE TABLE matching_results (
    result_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    matched_address_id VARCHAR(50),
    confidence_score DECIMAL(4,3),
    match_type VARCHAR(20),
    original_address TEXT,
    normalized_address TEXT,
    matched_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY (matched_address_id) REFERENCES canonical_addresses(hhid)
);

-- Indexes
CREATE INDEX idx_transactions_normalized ON transactions(normalized_address);
CREATE INDEX idx_canonical_normalized ON canonical_addresses(normalized_address);
CREATE INDEX idx_matching_transaction ON matching_results(transaction_id);
CREATE INDEX idx_matching_address ON matching_results(matched_address_id);