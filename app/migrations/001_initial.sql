CREATE TABLE transactions (
    transaction_id VARCHAR(128) PRIMARY KEY,
    source_account VARCHAR(128) NOT NULL,
    destination_account VARCHAR(128) NOT NULL,
    amount NUMERIC(20,4) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status VARCHAR(10) NOT NULL DEFAULT 'PROCESSING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    ready_at TIMESTAMPTZ NOT NULL DEFAULT (statement_timestamp() + INTERVAL '30 seconds'),
    processed_at TIMESTAMPTZ,
    CONSTRAINT valid_processing_deadline CHECK (ready_at >= created_at + INTERVAL '30 seconds'),
    CONSTRAINT valid_transaction_state CHECK (
        (status = 'PROCESSING' AND processed_at IS NULL) OR
        (status = 'PROCESSED' AND processed_at IS NOT NULL AND processed_at >= ready_at)
    )
);

CREATE INDEX transactions_pending_idx ON transactions (ready_at, transaction_id)
    WHERE status = 'PROCESSING';
