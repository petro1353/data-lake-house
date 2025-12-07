package main

import (
	"context"
	pb "duckdb-embedded/grpc/GrpcServer"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"strings"
	"time"
)

func convertProtoValue(v *pb.Value, colType string) interface{} {
	switch k := v.Kind.(type) {
	case *pb.Value_NullValue:
		return "NULL"
	case *pb.Value_DoubleValue:
		return k.DoubleValue
	case *pb.Value_Int64Value:
		return k.Int64Value
	case *pb.Value_BoolValue:
		return k.BoolValue
	case *pb.Value_StringValue:
		return k.StringValue
	case *pb.Value_BytesValue:
		// Convert bytes to a human-readable string, e.g., base64 or hex.
		// For now, let's just use fmt.Sprintf to show its representation.
		return fmt.Sprintf("%x", k.BytesValue)
	case *pb.Value_TimestampValue:
		t := k.TimestampValue.AsTime()
		switch colType {
		case "DATE":
			return t.Format("2006-01-02")
		case "TIMESTAMP": // This handles DuckDB's DATETIME
			// hh24:mi:ss.SSS
			return t.Format("2006-01-02 15:04:05.000")
		default:
			return t.Format(time.RFC3339) // Fallback for any other timestamp-like types
		}
	case *pb.Value_DurationValue:
		// Format as hh24:mi:ss.SSS
		d := k.DurationValue.AsDuration()
		totalMillis := d.Milliseconds()
		hours := totalMillis / (1000 * 60 * 60)
		totalMillis %= (1000 * 60 * 60)
		minutes := totalMillis / (1000 * 60)
		totalMillis %= (1000 * 60)
		seconds := totalMillis / 1000
		milliseconds := totalMillis % 1000
		return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds)
	case *pb.Value_IntervalValue:
		return fmt.Sprintf("months:%d, days:%d, micros:%d", k.IntervalValue.Months, k.IntervalValue.Days, k.IntervalValue.Micros)
	case *pb.Value_ListValue:
		var sb strings.Builder
		sb.WriteString("[")
		for i, v := range k.ListValue.Values {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Recursive call, but we don't know the subtype, so pass an empty type string
			sb.WriteString(fmt.Sprintf("%v", convertProtoValue(v, "")))
		}
		sb.WriteString("]")
		return sb.String()
	case *pb.Value_MapValue:
		var sb strings.Builder
		sb.WriteString("{")
		i := 0
		for key, v := range k.MapValue.Fields {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Recursive call, but we don't know the subtype, so pass an empty type string
			sb.WriteString(fmt.Sprintf("%s: %v", key, convertProtoValue(v, "")))
			i++
		}
		sb.WriteString("}")
		return sb.String()
	default:
		return nil
	}
}

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSQLServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	sql := `
		SELECT 
			'world' as hello, 
			123 as a, 
			1.2 as b, 
			true as c, 
			DATE '1992-09-20' as d, 
			TIME '01:02:03.456' as e,
			TIMESTAMP '1992-09-20 01:02:03.789' as f,
			INTERVAL 5 DAY as g,
			NULL as h,
            CAST('some bytes' AS BLOB) as i,
            'another string' as j,
			[1, 2, 3] as k,
			STRUCT_PACK(name := 'John', age := 25) as l,
    		MAP(['key1', 'key2'], ['value1', 'value2']) as m,
    		'{"name": "json_name", "age": 30}'::JSON as n
		UNION ALL
		SELECT 
			'world2' as hello, 
			124 as a, 
			1.3 as b, 
			false as c, 
			DATE '2023-11-01' as d, 
			TIME '14:30:45.123' as e,
			TIMESTAMP '2023-11-01 14:30:45.010' as f,
			INTERVAL 2 MONTH as g,
			NULL as h,
            CAST('other bytes' AS BLOB) as i,
            'last string' as j,
			['a', 'b', 'c'] as k,
			STRUCT_PACK(name := 'Jane', age := 99) as l,
			MAP(['k3'], ['v3']) as m,
			'{"city": "sf"}'::JSON as n
	`
	stream, err := client.ExecuteQuery(ctx, &pb.QueryRequest{Sql: sql})
	if err != nil {
		log.Fatalf("could not execute query: %v", err)
	}

	var columns []*pb.Column
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error receiving: %v", err)
		}

		switch r := res.Result.(type) {
		case *pb.ExecuteQueryResponse_Header:
			columns = r.Header.Columns
		case *pb.ExecuteQueryResponse_Row:
			var sb strings.Builder
			sb.WriteString("{ ")
			for i, val := range r.Row.Values {
				if i > 0 {
					sb.WriteString(", ")
				}
				// Pass the column type name to convertProtoValue
				sb.WriteString(fmt.Sprintf("%s(%s): %v", columns[i].Name, columns[i].Type, convertProtoValue(val, columns[i].Type)))
			}
			sb.WriteString(" }")
			log.Println(sb.String())
		}
	}
}
