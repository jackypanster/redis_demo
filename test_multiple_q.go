package main

import (
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jackypanster/util"
)

const (
	Queue = "queue_%d"
	Total = 100000
	Size  = 1
)

func getQ(id int) string {
	return fmt.Sprintf(Queue, id)
}

func Create(total int) {
	c := pool.Get()
	defer c.Close()

	for i := 1; i <= total; i++ {
		id := i % Size
		q := getQ(id)
		_, err := c.Do("RPUSH", q, i)
		if err != nil {
			log.Panicf("fail to RPUSH %+v", err)
		}
		time.Sleep(time.Millisecond)
	}
	fmt.Printf("done %d\n", total)
}

func Process(out chan int) {
	c := pool.Get()
	defer c.Close()

	for {
		id := util.RandomInt(0, 1024) % Size
		q := getQ(id)
		n, err := redis.Int(c.Do("LPOP", q))
		if err != nil && err != redis.ErrNil {
			log.Panic("fail to LPOP %+v", err)
		}
		if err == nil && n > 0 {
			time.Sleep(time.Millisecond * 10)
			out <- n
		}
	}
}
