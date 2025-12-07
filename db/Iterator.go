package db

import "database/sql"

type SqlIterator struct {
	rows    *sql.Rows
	columns []*sql.ColumnType
}

func NewSqlIterator(rows *sql.Rows) *SqlIterator {
	cols, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	return &SqlIterator{
		rows:    rows,
		columns: cols,
	}
}

func (iter *SqlIterator) Columns() []*sql.ColumnType {
	return iter.columns
}

func (iter *SqlIterator) Next() ([]interface{}, error) {
	if iter.rows.Next() {
		values := make([]interface{}, len(iter.columns))
		valuePtrs := make([]interface{}, len(iter.columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := iter.rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		return values, nil
	}

	return nil, sql.ErrNoRows
}
