// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// e2e harness on the original Go Arrow Flight SQL driver
// (github.com/apache/arrow-go, gRPC-Go stack): it seeds its own schema over
// the wire and drives SELECT / LIMIT / a parameterized query / an INSERT.
// Flight SQL has no place field, so every table statement names its tables in
// full, `local.flight_e2e_go.t`.
// Usage: go run . host:port
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver"
)

const db = "flight_e2e_go"

// tables are named in full: the schema of the clients' own database `local`.
func table(name string) string {
	return "local." + db + "." + name
}

func check(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", what, err)
		os.Exit(1)
	}
}

func exec(dbh *sql.DB, query string) int64 {
	res, err := dbh.Exec(query)
	check(err, "exec: "+query)
	n, err := res.RowsAffected()
	check(err, "rows affected: "+query)
	return n
}

// dropBestEffort ignores its error: the cleanup of a leftover of an aborted run
// must not fail the run.
func dropBestEffort(dbh *sql.DB, query string) {
	if _, err := dbh.Exec(query); err != nil {
		fmt.Println("cleanup:", query, "->", err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: flightsql_go host:port")
		os.Exit(2)
	}
	addr := os.Args[1]

	dbh, err := sql.Open("flightsql", "flightsql://"+addr)
	check(err, "open")
	defer dbh.Close()

	dropBestEffort(dbh, "DROP SCHEMA IF EXISTS "+db+" CASCADE")
	exec(dbh, "CREATE SCHEMA "+db)
	exec(dbh, "CREATE TABLE "+table("numbers")+" (id BIGINT, name STRING, score DOUBLE)")
	for i := 0; i < 8; i++ {
		exec(dbh, fmt.Sprintf("INSERT INTO %s (id, name, score) VALUES (%d, 'row-%d', %d.5)", table("numbers"), i, i, i))
	}

	// 1) SELECT + LIMIT
	rows, err := dbh.Query("SELECT * FROM " + table("numbers") + " LIMIT 5")
	check(err, "select")
	count := 0
	for rows.Next() {
		count++
		var id int64
		var name sql.NullString
		var score sql.NullFloat64
		check(rows.Scan(&id, &name, &score), "scan")
		if id < 0 || id >= 8 {
			fmt.Fprintf(os.Stderr, "FAIL select: unexpected id %d\n", id)
			os.Exit(1)
		}
	}
	check(rows.Err(), "rows iter")
	if count != 5 {
		fmt.Fprintf(os.Stderr, "FAIL limit: got %d rows, want 5\n", count)
		os.Exit(1)
	}
	fmt.Println("select+limit ok")

	// 2) a parameterized query
	rows2, err := dbh.Query("SELECT * FROM "+table("numbers")+" WHERE id = $1", int64(3))
	check(err, "prepared select")
	names := 0
	for rows2.Next() {
		var id int64
		var name sql.NullString
		var score sql.NullFloat64
		check(rows2.Scan(&id, &name, &score), "scan prepared")
		if !name.Valid || name.String != "row-3" {
			fmt.Fprintf(os.Stderr, "FAIL prepared: got %q, want row-3\n", name.String)
			os.Exit(1)
		}
		names++
	}
	check(rows2.Err(), "rows2 iter")
	if names != 1 {
		fmt.Fprintf(os.Stderr, "FAIL prepared rows: %d\n", names)
		os.Exit(1)
	}
	fmt.Println("prepared ok")

	// 3) INSERT (Exec)
	if n := exec(dbh, "INSERT INTO "+table("numbers")+" (id, name, score) VALUES (900, 'GoE2E', 7.5)"); n != 1 {
		fmt.Fprintf(os.Stderr, "FAIL insert affected: %d\n", n)
		os.Exit(1)
	}
	fmt.Println("insert ok")

	exec(dbh, "DROP SCHEMA "+db+" CASCADE")
	fmt.Println("GO E2E OK")
}