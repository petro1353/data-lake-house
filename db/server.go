package db

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
)

type DbServer struct {
	db *sql.DB
}

var (
	dbServerInstance *DbServer
	serverOnce       sync.Once
	serverDriver     = "duckdb"
	serverDatasource = "example.duckdb"
)

func GetInstance() *DbServer {
	serverOnce.Do(func() {
		server, err := sql.Open(serverDriver, serverDatasource)
		if err != nil {
			log.Fatalf("failed to open DuckDB: %v", err)
		}
		
		dbServerInstance = &DbServer{db: server}
	})

	return dbServerInstance
}

func (server *DbServer) Query(query string) (*SqlIterator, error) {
	rows, err := server.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query data: %v", err)
	}

	return NewSqlIterator(rows), nil
}
