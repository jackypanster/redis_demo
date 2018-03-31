package main

import (
	"log"
	"time"
)

func create(total int, out chan int) {
	c := pool.Get()
	defer c.Close()
	for i := 1; i <= total; i++ {
		_, err := c.Do("RPUSH", "Jobs", i)
		if err != nil {
			log.Panicf("fail to RPUSH %+v", err)
		}
		time.Sleep(time.Millisecond)
		do(out)
	}
}
