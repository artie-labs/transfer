package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
)

type TestCase struct {
	schema        string
	tableName     string
	expectedCount int
	rows          []map[string]any
}

var dsns = []string{
	"postgres://postgres:postgres@localhost:5432/destination_e2e?sslmode=disable",
	"postgres://postgres:postgres@localhost:5432/destination_e2e_fgo?sslmode=disable",
}

var testCases = []TestCase{
	{
		schema:        "artie",
		tableName:     "customers",
		expectedCount: 4,
		rows: []map[string]any{
			{
				"id":         1001,
				"first_name": "Sally",
				"last_name":  "Thomas",
				"email":      "sally.thomas@acme.com",
			},
			{
				"id":         1002,
				"first_name": "George",
				"last_name":  "Bailey",
				"email":      "gbailey@foobar.com",
			},
			{
				"id":         1003,
				"first_name": "Edward",
				"last_name":  "Walker",
				"email":      "ed@walker.com",
			},
			{
				"id":         1004,
				"first_name": "Anne",
				"last_name":  "Kretchmar",
				"email":      "annek@noanswer.org",
			},
		},
	},
	{
		schema:        "public",
		tableName:     "products",
		expectedCount: 9,
		rows: []map[string]any{
			{
				"id":          101,
				"name":        "scooter",
				"description": "Small 2-wheel scooter",
				"weight":      3.14,
			},
			{
				"id":          102,
				"name":        "car battery",
				"description": "12V car battery",
				"weight":      8.1,
			},
			{
				"id":          103,
				"name":        "12-pack drill bits",
				"description": "12-pack of drill bits with sizes ranging from #40 to #3",
				"weight":      0.8,
			},
			{
				"id":          104,
				"name":        "hammer",
				"description": "12oz carpenter's hammer",
				"weight":      0.75,
			},
			{
				"id":          105,
				"name":        "hammer",
				"description": "14oz carpenter's hammer",
				"weight":      0.875,
			},
			{
				"id":          106,
				"name":        "hammer",
				"description": "16oz carpenter's hammer",
				"weight":      1.0,
			},
			{
				"id":          107,
				"name":        "rocks",
				"description": "box of assorted rocks",
				"weight":      5.3,
			},
			{
				"id":          108,
				"name":        "jacket",
				"description": "water resistent black wind breaker",
				"weight":      0.1,
			},
			{
				"id":          109,
				"name":        "spare tire",
				"description": "24 inch spare tire",
				"weight":      22.2,
			},
		},
	},
}

func testCountRows(ctx context.Context, testCase TestCase) error {
	for _, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("unable to connect to database: %w", err)
		}
		defer conn.Close(ctx)

		var count int
		query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, testCase.schema, testCase.tableName)
		if err = conn.QueryRow(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("failed to count rows in %s.%s: %w", testCase.schema, testCase.tableName, err)
		}

		if count != testCase.expectedCount {
			return fmt.Errorf("row count mismatch for %s.%s: got %d, expected %d", testCase.schema, testCase.tableName, count, testCase.expectedCount)
		}
	}
	return nil
}

func testCustomerRows(ctx context.Context, testCase TestCase) error {
	for _, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("unable to connect to database: %w", err)
		}
		defer conn.Close(ctx)

		query := fmt.Sprintf(`SELECT id, first_name, last_name, email FROM "%s"."%s"`, testCase.schema, testCase.tableName)
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query rows in %s.%s: %w", testCase.schema, testCase.tableName, err)
		}
		defer rows.Close()

		tableData := make(map[int]map[string]any)
		for rows.Next() {
			var id int
			var firstName string
			var lastName string
			var email string
			if err = rows.Scan(&id, &firstName, &lastName, &email); err != nil {
				return fmt.Errorf("failed to scan row in %s.%s: %w", testCase.schema, testCase.tableName, err)
			}

			tableData[id] = map[string]any{
				"id":         id,
				"first_name": firstName,
				"last_name":  lastName,
				"email":      email,
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows in %s.%s: %w", testCase.schema, testCase.tableName, err)
		}

		for _, row := range testCase.rows {
			rowMap, ok := tableData[row["id"].(int)]
			if !ok {
				return fmt.Errorf("row with id %v not found in %s.%s", row["id"], testCase.schema, testCase.tableName)
			}
			if !reflect.DeepEqual(rowMap, row) {
				return fmt.Errorf("row mismatch for id %v in %s.%s: got %v, expected %v", row["id"], testCase.schema, testCase.tableName, rowMap, row)
			}
		}
	}
	return nil
}

func testProductRows(ctx context.Context, testCase TestCase) error {
	for _, dsn := range dsns {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("unable to connect to database: %w", err)
		}

		defer conn.Close(ctx)

		// Store the whole table in a map keyed by id
		query := fmt.Sprintf(`SELECT id, name, description, weight FROM "%s"."%s"`, testCase.schema, testCase.tableName)
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query rows in %s.%s: %w", testCase.schema, testCase.tableName, err)
		}
		defer rows.Close()

		tableData := make(map[int]map[string]any)
		for rows.Next() {
			var id int
			var name string
			var description *string
			var weight *float64
			if err := rows.Scan(&id, &name, &description, &weight); err != nil {
				return fmt.Errorf("failed to scan row in %s.%s: %w", testCase.schema, testCase.tableName, err)
			}

			rowMap := make(map[string]any)
			rowMap["id"] = id
			rowMap["name"] = name
			if description != nil {
				rowMap["description"] = *description
			} else {
				rowMap["description"] = nil
			}
			if weight != nil {
				rowMap["weight"] = *weight
			} else {
				rowMap["weight"] = nil
			}
			tableData[id] = rowMap
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows in %s.%s: %w", testCase.schema, testCase.tableName, err)
		}

		for _, row := range testCase.rows {
			rowMap, ok := tableData[row["id"].(int)]
			if !ok {
				return fmt.Errorf("row with id %v not found in %s.%s", row["id"], testCase.schema, testCase.tableName)
			}
			if !reflect.DeepEqual(rowMap, row) {
				return fmt.Errorf("row mismatch for id %v in %s.%s: got %v, expected %v", row["id"], testCase.schema, testCase.tableName, rowMap, row)
			}
		}
	}
	return nil
}

func main() {
	ctx := context.Background()

	const maxIterations = 10
	for i := 1; i <= maxIterations; i++ {
		err1 := testCountRows(ctx, testCases[0])
		err2 := testCustomerRows(ctx, testCases[0])
		err3 := testCountRows(ctx, testCases[1])
		err4 := testProductRows(ctx, testCases[1])

		if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
			log.Println("All tests passed successfully.")
			break
		}

		log.Printf("Iteration %d: Some tests failed. Retrying in 5 seconds...\n", i)
		if err1 != nil {
			log.Printf("testCountRows (customers) error: %v", err1)
		}
		if err2 != nil {
			log.Printf("testCustomerRows error: %v", err2)
		}
		if err3 != nil {
			log.Printf("testCountRows (products) error: %v", err3)
		}
		if err4 != nil {
			log.Printf("testProductRows error: %v", err4)
		}

		if i == maxIterations {
			log.Fatal("Exceeded maximum number of iterations. Exiting.")
		}
		// Wait 5 seconds before next attempt
		select {
		case <-ctx.Done():
			log.Fatal("Context cancelled, exiting.")
		case <-time.After(5 * time.Second):
		}
	}
}
