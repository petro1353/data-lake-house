package grpc

import (
	"database/sql"
	"duckdb-embedded/db"
	pb "duckdb-embedded/grpc/GrpcServer"
	"errors"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type SqlServer struct {
	pb.UnimplementedSQLServiceServer
	*db.DbServer
}

func (s *SqlServer) ExecuteQuery(req *pb.QueryRequest, stream pb.SQLService_ExecuteQueryServer) error {
	sqlStmt := req.GetSql()
	if sqlStmt == "" {
		return errors.New("sql string is empty")
	}

	iter, err := s.DbServer.Query(sqlStmt)
	if err != nil {
		return fmt.Errorf("error executing query: %v", err)
	}

	colTypes := iter.Columns()
	pbCols := make([]*pb.Column, len(colTypes))
	for i, ct := range colTypes {
		pbCols[i] = &pb.Column{Name: ct.Name(), Type: ct.DatabaseTypeName()}
	}

	header := &pb.Header{Columns: pbCols}
	if err := stream.Send(&pb.ExecuteQueryResponse{Result: &pb.ExecuteQueryResponse_Header{Header: header}}); err != nil {
		return err
	}

	for {
		rowValues, err := iter.Next()
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		protoRow, err := convertRowToProto(rowValues, colTypes)
		if err != nil {
			return err
		}

		if err := stream.Send(&pb.ExecuteQueryResponse{Result: &pb.ExecuteQueryResponse_Row{Row: protoRow}}); err != nil {
			return err
		}
	}
}

func convertRowToProto(rowValues []interface{}, colTypes []*sql.ColumnType) (*pb.Row, error) {
	protoRow := &pb.Row{Values: make([]*pb.Value, len(rowValues))}
	for i, v := range rowValues {
		val, err := convertValueToProto(v, colTypes[i])
		if err != nil {
			return nil, fmt.Errorf("error converting value at index %d: %w", i, err)
		}
		protoRow.Values[i] = val
	}
	return protoRow, nil
}

func convertValueToProto(v interface{}, colType *sql.ColumnType) (*pb.Value, error) {
	if v == nil {
		return &pb.Value{Kind: &pb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
	}

	if colType != nil {
		switch colType.DatabaseTypeName() {
		case "TIMESTAMP", "DATE":
			if t, ok := v.(time.Time); ok {
				return &pb.Value{Kind: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}, nil
			}
		case "TIME":
			if t, ok := v.(time.Time); ok {
				// A "zero" date is 1970-01-01 for time.Time, so we get the duration from midnight on that day.
				nanos := t.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
				return &pb.Value{Kind: &pb.Value_DurationValue{DurationValue: durationpb.New(nanos)}}, nil
			}
		case "INTERVAL":
			if i, ok := v.(duckdb.Interval); ok {
				return &pb.Value{Kind: &pb.Value_IntervalValue{IntervalValue: &pb.Interval{
					Months: i.Months,
					Days:   i.Days,
					Micros: i.Micros,
				}}}, nil
			}
		}
	}

	// Fallback to type sniffing for other types
	switch val := v.(type) {
	case map[string]interface{}:
		mapVal := &pb.MapValue{Fields: make(map[string]*pb.Value, len(val))}
		for key, item := range val {
			// Recursive call. Pass nil for colType as we don't know the sub-type.
			protoVal, err := convertValueToProto(item, nil)
			if err != nil {
				return nil, err
			}
			mapVal.Fields[key] = protoVal
		}
		return &pb.Value{Kind: &pb.Value_MapValue{MapValue: mapVal}}, nil
	case []interface{}:
		listVal := &pb.ListValue{Values: make([]*pb.Value, len(val))}
		for i, item := range val {
			// Recursive call. Pass nil for colType as we don't know the sub-type.
			// The conversion will rely on Go type-sniffing for the elements.
			protoVal, err := convertValueToProto(item, nil)
			if err != nil {
				return nil, err
			}
			listVal.Values[i] = protoVal
		}
		return &pb.Value{Kind: &pb.Value_ListValue{ListValue: listVal}}, nil
	case int64:
		return &pb.Value{Kind: &pb.Value_Int64Value{Int64Value: val}}, nil
	case float64:
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: val}}, nil
	case duckdb.Decimal:
		f := val.Float64()
		return &pb.Value{Kind: &pb.Value_DoubleValue{DoubleValue: f}}, nil
	case bool:
		return &pb.Value{Kind: &pb.Value_BoolValue{BoolValue: val}}, nil
	case string:
		return &pb.Value{Kind: &pb.Value_StringValue{StringValue: val}}, nil
	case []byte:
		return &pb.Value{Kind: &pb.Value_BytesValue{BytesValue: val}}, nil
	default:
		// As a last resort, convert to string
		return &pb.Value{Kind: &pb.Value_StringValue{StringValue: fmt.Sprint(v)}}, nil
	}
}
