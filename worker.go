package main

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jackypanster/util"
)

var (
	pool *redis.Pool
)

const (
	Total = 100000
)

func init() {
	util.InitQueue(32, Total)
	pool = util.NewRedisPool("127.0.0.1", 6379)
}

func do(out chan int) {
	util.JobQueue <- util.Job{
		Do: func() error {
			c := pool.Get()
			defer c.Close()
			//for {
			n, err := redis.Int(c.Do("LPOP", "Jobs"))
			if err != nil && err != redis.ErrNil {
				log.Panic("fail to LPOP %+v", err)
			}
			if err == nil && n > 0 {
				out <- n
				time.Sleep(time.Millisecond * 10)
			}
			//}
			return nil
		},
	}
}

func main() {
	out := make(chan int, Total)
	start := time.Now()
	count := 0
	go create(Total, out)
	for {
		select {
		case n, ok := <-out:
			if ok && n > 0 {
				count++
				if count == Total {
					log.Printf("%s", time.Now().Sub(start))
					return
				}
			}
		}
	}
}
