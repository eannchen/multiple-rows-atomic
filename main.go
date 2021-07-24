package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB

func init() {
	var err error
	db, err = gorm.Open(postgres.Open(
		"host=localhost user=postgres password=1234567890 dbname=postgres sslmode=disable port=5432"),
		&gorm.Config{Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{SlowThreshold: time.Second},
		)},
	)
	if err != nil {
		log.Fatal("failed to connect database")
	}

	err = db.Exec("CREATE TABLE IF NOT EXISTS t AS SELECT i, substr(md5(random()::text), 0, 25) AS s FROM generate_series(1,1000000) s(i);").Error
	if err != nil {
		log.Fatal("seed db error")
	}
}

func main() {
	all_deadlock()
	// sizeCond_deadlock()
	// likeCond_deadlock()
	// diffOrder_deadlock()

	// sameOrder_ok()
	// andCond_ok()
	// inCond_ok()
}

func all_deadlock() {
	concurrency(func(tx *gorm.DB, _ int) error {
		return tx.Exec("SELECT i FROM t FOR UPDATE;").Error
	})
}

func sizeCond_deadlock() {
	concurrency(func(tx *gorm.DB, _ int) error {
		return tx.Exec("SELECT i FROM t WHERE i > 10 FOR UPDATE;").Error
	})
}

func likeCond_deadlock() {
	concurrency(func(tx *gorm.DB, _ int) error {
		return tx.Exec("SELECT i FROM t WHERE s LIKE '1%' FOR UPDATE;").Error
	})
}

func diffOrder_deadlock() {
	concurrency(func(tx *gorm.DB, i int) error {
		if i%2 == 0 {
			return tx.Exec("SELECT i FROM t ORDER BY i FOR UPDATE;").Error
		} else {
			return tx.Exec("SELECT i FROM t ORDER BY i DESC FOR UPDATE;").Error
		}
	})
}

func sameOrder_ok() {
	concurrency(func(tx *gorm.DB, _ int) error {
		return tx.Exec("SELECT i FROM t ORDER BY i FOR UPDATE;").Error
	})
}

func andCond_ok() {
	concurrency(func(tx *gorm.DB, _ int) error {
		slice := makeRange(1, 100)
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(slice), func(i, j int) { slice[i], slice[j] = slice[j], slice[i] })

		s := fmt.Sprintf("i = %d", slice[0])
		for _, n := range slice[1:] {
			s += fmt.Sprintf(" AND i = %d", n)
		}
		return tx.Exec(fmt.Sprintf("SELECT i FROM t WHERE %s FOR UPDATE;", s)).Error
	})
}

func inCond_ok() {
	concurrency(func(tx *gorm.DB, _ int) error {
		slice := makeRange(1, 1000)
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(slice), func(i, j int) { slice[i], slice[j] = slice[j], slice[i] })
		return tx.Exec("SELECT i FROM t WHERE i IN (?) FOR UPDATE;", slice).Error
	})
}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

func concurrency(fn func(tx *gorm.DB, i int) error) {
	var wg sync.WaitGroup
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := db.Begin()

			if err := fn(tx, i); err != nil {
				tx.Rollback()
				fmt.Println(errors.Wrap(err, "select err").Error())
				return
			}

			if err := tx.Commit().Error; err != nil {
				fmt.Printf("commit err: %s\n", err.Error())
			}
		}(i)
	}
	wg.Wait()
}
