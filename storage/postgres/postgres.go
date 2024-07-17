package postgres

import (
	"Content-Service/config"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

func ConnectDB() (*sql.DB, error) {
	cfg := config.Load()

	conn := fmt.Sprintf("port = %s host=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Postgres.DB_PORT, cfg.Postgres.DB_HOST, cfg.Postgres.DB_USER, cfg.Postgres.DB_PASSWORD, cfg.Postgres.DB_NAME)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
