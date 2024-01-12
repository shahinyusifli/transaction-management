CREATE SCHEMA bronze;

CREATE TABLE bronze.transactions (
    user_id VARCHAR(255),
    account_id VARCHAR(255),
    counterparty_id VARCHAR(255),
    transaction_type VARCHAR(255),
    date VARCHAR(255),
    amount VARCHAR(255)
);

CREATE SCHEMA silver;

CREATE TABLE silver.transactions (
    user_id VARCHAR(255),
    account_id VARCHAR(255),
    counterparty_id VARCHAR(255),
    transaction_type VARCHAR(255),
    date DATE,
    amount INT
);

CREATE SCHEMA gold;

CREATE TABLE gold.transactions_by_max_money (
    user_id VARCHAR(255),
    counterparty_id VARCHAR(255),
    amount INT
);
