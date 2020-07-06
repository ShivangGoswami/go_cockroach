package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "postgresql://maxroach@localhost:26257/bank?sslmode=disable")
	if err != nil {
		log.Fatal("error connecting to the database:", err)
	}

	if _, err := db.Exec("create table if not exists accounts(id INT PRIMARY KEY,balance INT)"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec("insert into accounts (id,balance) values (1,1000), (2,250)"); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("Select id,balance from accounts")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	fmt.Println("Initial balances:")
	for rows.Next() {
		var id, balance int
		if err := rows.Scan(&id, &balance); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d %d\n", id, balance)
	}
}
