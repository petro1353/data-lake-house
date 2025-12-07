package handler

import (
	"database/sql"
	"duckdb-embedded/db"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	query, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to read request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	dbInst := db.GetInstance()
	iter, err := dbInst.Query(string(query))
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to execute query: %s", err.Error()), http.StatusBadRequest)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	columns := iter.Columns()

	w.Write([]byte("["))
	first := true
	for {
		rowValues, err := iter.Next()
		if errors.Is(err, sql.ErrNoRows) {
			w.Write([]byte("]"))
			return
		}

		if !first {
			w.Write([]byte(","))
		}
		first = false

		rowMap := make(map[string]interface{})
		for i, colType := range columns {
			rowMap[colType.Name()] = rowValues[i]
		}

		jsonRow, err := json.Marshal(rowMap)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(jsonRow)
	}
}
