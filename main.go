package main

import (
	"database/sql"
	duckDb "duckdb-embedded/db"
	duckDbGrpc "duckdb-embedded/grpc"
	"duckdb-embedded/handler"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"

	pb "duckdb-embedded/grpc/GrpcServer"
	"github.com/gorilla/mux"
	_ "github.com/marcboeker/go-duckdb"
)

var (
	db             *sql.DB
	httpServerPort = 8080
	grpcServerPort = 50051
)

func main() {
	httpServerDone := make(chan bool)
	grpcServerDone := make(chan bool)

	go startHttpServer(httpServerDone)
	go startGrpcServer(grpcServerDone)

	<-httpServerDone
	<-grpcServerDone
}

func startHttpServer(httpServerDone chan bool) {
	// Create a new router
	r := mux.NewRouter()

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
	r.HandleFunc("/search", handler.QueryHandler).Methods("POST")

	// Start the HTTP server
	log.Printf("✅ HTTP Server listening on :%d\n", httpServerPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpServerPort), r); err != nil {
		log.Fatalf("HTTP Server encountered a fatal error and stopped: %v", err)
	}

	httpServerDone <- true
}

func startGrpcServer(grpcServerDone chan bool) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcServerPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSQLServiceServer(grpcServer, &duckDbGrpc.SqlServer{DbServer: duckDb.GetInstance()})

	log.Printf("✅ gRPC server listening on :%d\n", grpcServerPort)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server encountered a fatal error and stopped: %v", err)
	}

	grpcServerDone <- true
}

func tmp() {
	// Open (or create) a DuckDB file
	db, err := sql.Open("duckdb", "example.duckdb")
	if err != nil {
		log.Fatalf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT);`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Insert some data
	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');`)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Query the data
	rows, err := db.Query(`SELECT id, name FROM users;`)
	if err != nil {
		log.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatalf("failed to read row: %v", err)
		}
		fmt.Printf(" - %d: %s\n", id, name)
	}
}

func startDb() {
	a, err := sql.Open("duckdb", "example.duckdb")
	if err != nil {
		log.Fatalf("failed to open DuckDB: %v", err)
	}

	db = a
}
