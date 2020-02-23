package database

import (
	"context"
	"database/sql"
	"fmt"
	"frames_generator/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"log"
	"sync"
	"time"
)

var (
	once     sync.Once
	connPool *sqlx.DB
)

func init() {
	config.SetupConfig()
	setupConnectionPool()
}

// Connects to a database via given credentials and returns connection pool entity
func setupConnectionPool() *sqlx.DB {
	once.Do(func() {
		var err error
		user := viper.GetString("pg.user")
		password := viper.GetString("pg.password")
		dbName := viper.GetString("pg.database")
		fmt.Println(fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", user, password, dbName))
		if connPool, err = sqlx.Connect("postgres", fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", user, password, dbName)); err != nil {
			log.Panic(err)
		}
		connPool.DB.SetMaxOpenConns(viper.GetInt("pg.max_connections_limit"))
		connPool.DB.SetMaxIdleConns(viper.GetInt("pg.max_idle_connections"))
		connPool.DB.SetConnMaxLifetime(time.Duration(viper.GetInt("pg.max_connection_lifetime")))
	})

	return connPool
}

// Setup a transaction with necessary setting and a cancel func.The cancel func call is applicable to rollback the transaction
func SetupTransaction(conn *sql.Conn) (*sql.Tx, context.CancelFunc, error) {
	// cancelFunc call rollbacks the entire transaction
	ctx, cancelFunc := context.WithCancel(context.Background())
	transaction, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: 2,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Transaction opening has failed")
	}

	return transaction, cancelFunc, nil
}

// Returns single connection to database
func SetupDBConnection() (*sql.Conn, error) {
	conn, err := connPool.Conn(context.Background())
	if err != nil {
		return nil, errors.WithMessage(err, "Connection establishing has failed")
	}

	return conn, nil
}
