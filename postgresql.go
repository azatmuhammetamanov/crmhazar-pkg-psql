package crmhazar_pkg_psql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"time"
)

func NewClient(ctx context.Context, options Options) (client Client, err error) {

	log.Println("new client options")
	log.Println("🔔 Host: ", options.Host)
	log.Println("🔔 Port: ", options.Port)
	log.Println("🔔 Database: ", options.Database)
	log.Println("🔔 Username: ", options.Username)
	log.Println("🔔 Password: ", "*******")
	log.Println("🔔 PgPoolMaxConn: ", options.PgPoolMaxConn)

	connPool, err := pgxpool.NewWithConfig(ctx, getConfig(options))

	if err != nil {
		return nil, errors.New("🚫 Error while creating connection to the database!!")
	}

	connection, err := connPool.Acquire(ctx)

	if err != nil {
		return nil, errors.New("🚫 Error while acquiring connection from the database pool!!")
	}

	defer connection.Release()

	err = connection.Ping(ctx)

	if err != nil {
		return nil, errors.New("🚫 Could not ping database")
	}

	log.Println("✅ postgresql connected success")

	//return &Pool{connPool}, nil
	return connPool, nil
}

func getConfig(options Options) *pgxpool.Config {

	DatabaseUrl := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		options.Username,
		options.Password,
		options.Host,
		options.Port,
		options.Database)

	log.Println("🔔 database url: ", DatabaseUrl)

	dbConfig, err := pgxpool.ParseConfig(DatabaseUrl)
	if err != nil {
		log.Println("🚫 Failed to create a config, error: ", err)
	}

	dbConfig.MaxConns = int32(options.PgPoolMaxConn)
	dbConfig.MinConns = int32(0)
	dbConfig.MaxConnLifetime = time.Hour
	dbConfig.MaxConnIdleTime = time.Minute * 30
	dbConfig.HealthCheckPeriod = time.Minute
	dbConfig.ConnConfig.ConnectTimeout = time.Second * 5

	//dbConfig.BeforeAcquire = func(ctx context.Context, c *pgx.Conn) bool {
	//	return true
	//}
	//
	//dbConfig.AfterRelease = func(c *pgx.Conn) bool {
	//	return true
	//}
	//
	//dbConfig.BeforeClose = func(c *pgx.Conn) {
	//	fmt.Println("Closed the connection pool to the database!!")
	//}

	return dbConfig
}
