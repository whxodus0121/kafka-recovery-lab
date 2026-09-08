package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"kafka-recovery-lab/internal/config"
)

func Open(ctx context.Context) (*sql.DB, error) {
	db, err := Pool()
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("MySQL ping: %w", err)
	}
	return db, nil
}

// Pool validates configuration without requiring a live DB at worker startup.
// Connection failures then belong to an actual source record and its policy.
func Pool() (*sql.DB, error) {
	c, err := config.Database()
	if err != nil {
		return nil, err
	}
	d := driver.NewConfig()
	d.Net, d.Addr, d.User, d.Passwd, d.DBName = "tcp", c.Address, c.User, c.Password, c.Database
	d.Timeout = 5 * time.Second
	d.ReadTimeout, d.WriteTimeout = 10*time.Second, 10*time.Second
	connector, err := driver.NewConnector(d)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Minute)
	return db, nil
}
