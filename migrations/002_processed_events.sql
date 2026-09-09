CREATE TABLE IF NOT EXISTS processed_events (
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    payload_hash BINARY(32) NOT NULL,
    order_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    product_id BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    processed_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;
