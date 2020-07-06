package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
)

type Account struct {
	ID      int `gorm:"primary_key"`
	Balance int
}

type txnFunc func(*gorm.DB) error

var forceRetryLoop txnFunc = func(db *gorm.DB) error {
	if err := db.Exec("select now()").Error; err != nil {
		return err
	}
	if err := db.Exec("Select crdb_internal.force_retry('1s'::INTERVAL)").Error; err != nil {
		return err
	}
	return nil
}

func transferFunds(db *gorm.DB, fromID int, toID int, amount int) error {
	var fromAccount Account
	var toAccount Account

	db.First(&fromAccount, fromID)
	db.First(&toAccount, toID)

	if fromAccount.Balance < amount {
		return fmt.Errorf("account %d balance %d is lower than transfer amount %d", fromAccount.ID, fromAccount.Balance, amount)
	}

	fromAccount.Balance -= amount
	toAccount.Balance += amount

	if err := db.Save(&fromAccount).Error; err != nil {
		return err
	}
	if err := db.Save(&toAccount).Error; err != nil {
		return err
	}
	return nil
}

func main() {
	const addr = "postgresql://maxroach@localhost:26257/bank?sslmode=disable"
	db, err := gorm.Open("postgres", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.LogMode(false)
	db.AutoMigrate(&Account{})

	var fromID = 1
	var toID = 2
	db.Create(&Account{ID: fromID, Balance: 1000})
	db.Create(&Account{ID: toID, Balance: 250})

	printBalances(db)

	var amount = 100
	if err := runTransaction(db, func(*gorm.DB) error {
		return transferFunds(db, fromID, toID, amount)
	}); err != nil {
		fmt.Println(err)
	}
	printBalances(db)
	deleteAccounts(db)
}

func runTransaction(db *gorm.DB, fn txnFunc) error {
	var maxRetries = 3
	for retries := 0; retries <= maxRetries; retries++ {
		if retries == maxRetries {
			return fmt.Errorf("hit max of %d retries, aborting", retries)
		}
		txn := db.Begin()
		if err := fn(txn); err != nil {
			pqErr := err.(*pq.Error)
			if pqErr.Code == "40001" {
				txn.Rollback()
				var sleepMs = math.Pow(2, float64(retries)) * 100 * (rand.Float64() + 0.5)
				fmt.Printf("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMs)
				time.Sleep(time.Millisecond * time.Duration(sleepMs))
			} else {
				return err
			}
		} else {
			if err := txn.Commit().Error; err != nil {
				pqErr := err.(*pq.Error)
				if pqErr.Code == "40001" {
					continue
				} else {
					return err
				}
			}
			break
		}
	}
	return nil
}

func printBalances(db *gorm.DB) {
	var accounts []Account
	db.Find(&accounts)
	fmt.Printf("Balance at '%s':\n", time.Now())
	for _, account := range accounts {
		fmt.Printf("%d %d\n", account.ID, account.Balance)
	}
}

func deleteAccounts(db *gorm.DB) error {
	err := db.Exec("DELETE from accounts where ID > 0").Error
	if err != nil {
		return err
	}
	return nil
}
