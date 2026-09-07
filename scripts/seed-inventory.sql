-- Explicit local test reset. Stop API/worker and confirm no pending records first.
INSERT INTO inventory (product_id, available_quantity, updated_at)
VALUES (1, 100, UTC_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE available_quantity = 100, updated_at = UTC_TIMESTAMP(6);
