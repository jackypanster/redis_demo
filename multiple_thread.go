package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jackypanster/util"
)

var (
	pool *redis.Pool
)

func init() {
	util.InitQueue(1024, 1000000)
	pool = util.NewRedisPool("127.0.0.1", 6379)
}

func do(out chan int) {
	c := pool.Get()
	defer c.Close()
	for {
		n, err := redis.Int(c.Do("LPOP", "Jobs"))
		if err != nil && err != redis.ErrNil {
			log.Panic("fail to pop %s", err.Error())
		}
		if err != nil && err == redis.ErrNil {
			time.Sleep(time.Second)
			continue
		}
		if err == nil && n > 0 {
			util.JobQueue <- util.Job{
				Do: func() error {
					time.Sleep(time.Millisecond * 100)
					out <- n
					return nil
				},
			}
		}
	}
}

func main() {
	arg := os.Args[1]
	num, err := strconv.Atoi(arg)
	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(2)
	}

	out := make(chan int, num)
	start := time.Now()
	count := 0

	for i := 0; i < runtime.NumCPU(); i++ {
		go do(out)
	}

	for {
		select {
		case n, ok := <-out:
			if ok && n > 0 {
				count++
				fmt.Println(n)
				if count == num {
					log.Printf("%s", time.Now().Sub(start))
					return
				}
			}
		}
	}
}
