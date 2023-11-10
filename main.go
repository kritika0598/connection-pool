package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

const (
	username = "root"
	password = "secret"
	hostname = "127.0.0.1:3306"
	dbname   = "test"
)

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

// sets up a new connection with database and return
func newConn() *sql.DB {
	_db, err := sql.Open("mysql", dsn(dbname))
	if err != nil {
		panic(err)
	}
	return _db
}

type conn struct {
	db *sql.DB
}

type cpool struct {
	mu      *sync.Mutex
	channel chan interface{}
	conns   []*conn
	maxConn int
}

func NewCPool(maxConn int) (*cpool, error) {
	var mu = sync.Mutex{}
	pool := &cpool{
		mu:      &mu,
		conns:   make([]*conn, 0, maxConn),
		maxConn: maxConn,
		channel: make(chan interface{}, maxConn),
	}

	for i := 0; i < maxConn; i++ {
		pool.conns = append(pool.conns, &conn{newConn()})
		pool.channel <- nil
	}
	return pool, nil
}

func (pool *cpool) Close() {
	close(pool.channel)
	for i := range pool.conns {
		pool.conns[i].db.Close()
	}
}

func (pool *cpool) Get() (*conn, error) {
	// waiting for atleast one element to be there in the queue
	// tihs will block until something is put in channel
	<-pool.channel

	// take the exclusive lock on pool
	pool.mu.Lock()
	c := pool.conns[0]
	// remove 1st element from array
	pool.conns = pool.conns[1:]

	// release the lock
	pool.mu.Unlock()

	return c, nil
}

func (pool *cpool) Put(c *conn) {
	// adding nil to buffered queue, 1-1 correspondence between array of connection and channel
	// making this call before appending connection to pool array because this will block if channel has reached maxConn size
	pool.channel <- nil

	// take exclusive lock
	pool.mu.Lock()
	pool.conns = append(pool.conns, c)
	pool.mu.Unlock()
}

func benchmarkNonPool() {
	startTime := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		// new thread spin up
		go func() {
			defer wg.Done()

			db := newConn()
			// asking it to sleep for 10ms
			_, err := db.Exec("SELECT SLEEP(0.01);")
			if err != nil {
				panic(err)
			}

			db.Close()
		}()
	}
	wg.Wait()
	fmt.Println("Benchmark Non Connection Pool", time.Since(startTime))
}

func benchmarkPool() {
	startTime := time.Now()
	pool, err := NewCPool(10)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// get connection from pool
			connection, err := pool.Get()
			if err != nil {
				panic(err)
			}
			// asking it to sleep for 10ms
			_, err = connection.db.Exec("SELECT SLEEP(0.01);")
			if err != nil {
				panic(err)
			}

			// put connection back in pool
			pool.Put(connection)

		}()
	}
	wg.Wait()
	fmt.Println("Benchmark Connection Pool", time.Since(startTime))
	pool.Close()
}

func main() {
	benchmarkNonPool()
	benchmarkPool()
}
