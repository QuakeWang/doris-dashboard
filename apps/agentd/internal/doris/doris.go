package doris

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type ConnConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string

	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

func OpenDB(cfg ConnConfig) (*sql.DB, error) {
	if strings.TrimSpace(cfg.Host) == "" {
		return nil, errors.New("host is required")
	}
	if cfg.Port <= 0 {
		return nil, errors.New("port is required")
	}
	if strings.TrimSpace(cfg.User) == "" {
		return nil, errors.New("user is required")
	}
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	c := mysql.NewConfig()
	c.Net = "tcp"
	c.Addr = addr
	c.User = cfg.User
	c.Passwd = cfg.Password
	if database := strings.TrimSpace(cfg.Database); database != "" {
		c.DBName = database
	}
	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 5 * time.Second
	}
	rwTimeout := cfg.ReadTimeout
	if rwTimeout <= 0 {
		rwTimeout = 2 * time.Minute
	}
	if cfg.WriteTimeout > rwTimeout {
		rwTimeout = cfg.WriteTimeout
	}

	c.Timeout = connectTimeout
	c.ReadTimeout = rwTimeout
	c.WriteTimeout = rwTimeout
	// Doris clusters may disable server-side prepared statements.
	// Interpolate placeholders on client side so QueryContext with args still works.
	c.InterpolateParams = true
	c.Params = map[string]string{
		"charset": "utf8mb4",
	}

	db, err := sql.Open("mysql", c.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(2 * time.Minute)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	return db, nil
}

func openAndPing(ctx context.Context, cfg ConnConfig) (*sql.DB, error) {
	db, err := OpenDB(cfg)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func TestConnection(ctx context.Context, cfg ConnConfig) error {
	db, err := openAndPing(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	var probe int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&probe); err != nil {
		return err
	}
	if probe != 1 {
		return errors.New("unexpected SELECT 1 result")
	}
	return nil
}
