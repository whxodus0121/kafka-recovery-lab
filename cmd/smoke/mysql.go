package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"kafka-recovery-lab/internal/mysql"
)

func mysqlSmoke(ctx context.Context, command, id string) error {
	db, err := mysql.Open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if command == "mysql" {
		var one int
		var version, engine string
		if err := db.QueryRowContext(ctx, "SELECT 1, VERSION(), @@default_storage_engine").Scan(&one, &version, &engine); err != nil {
			return err
		}
		if one != 1 || !strings.EqualFold(engine, "InnoDB") {
			return fmt.Errorf("unexpected SQL result or storage engine")
		}
		fmt.Printf("MYSQL_PASS select=%d version=%s default_engine=%s\n", one, version, engine)
		return nil
	}
	// Phase 0 diagnostic table only. No business tables or migrations.
	if command == "mysql-write" {
		if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS phase0_probe (
			probe_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
			probe_value VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL
		) ENGINE=InnoDB`); err != nil {
			return err
		}
		if err := probeEngine(ctx, db); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO phase0_probe (probe_id, probe_value) VALUES (?, ?)", id, id); err != nil {
			return err
		}
		fmt.Printf("MYSQL_WRITE_PASS id=%s\n", id)
		return nil
	}
	if command == "mysql-read" {
		if err := probeEngine(ctx, db); err != nil {
			return err
		}
		var value string
		if err := db.QueryRowContext(ctx, "SELECT probe_value FROM phase0_probe WHERE probe_id = ?", id).Scan(&value); err != nil {
			return err
		}
		if value != id {
			return fmt.Errorf("persisted probe value mismatch")
		}
		fmt.Printf("MYSQL_READ_PASS id=%s value_match=true engine=InnoDB\n", id)
		return nil
	}
	result, err := db.ExecContext(ctx, "DELETE FROM phase0_probe WHERE probe_id = ?", id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return fmt.Errorf("expected exactly one probe row to clean, got %d: %v", rows, err)
	}
	fmt.Printf("MYSQL_CLEAN_PASS id=%s deleted=%d\n", id, rows)
	return nil
}

func probeEngine(ctx context.Context, db *sql.DB) error {
	var engine string
	if err := db.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'phase0_probe'").Scan(&engine); err != nil {
		return err
	}
	if !strings.EqualFold(engine, "InnoDB") {
		return fmt.Errorf("phase0_probe must use InnoDB")
	}
	return nil
}
