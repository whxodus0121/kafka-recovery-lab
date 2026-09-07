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
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("MySQL ping: %w", err)
	}
	return db, nil
}
