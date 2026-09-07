CREATE TABLE IF NOT EXISTS inventory (
    product_id BIGINT NOT NULL PRIMARY KEY,
    available_quantity BIGINT NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    CONSTRAINT inventory_positive_product CHECK (product_id > 0),
    CONSTRAINT inventory_nonnegative_quantity CHECK (available_quantity >= 0)
) ENGINE=InnoDB;
