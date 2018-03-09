package main

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jackypanster/util"
)

func Produce(count int) {
	c := pool.Get()
	defer c.Close()

	for i := 1; i <= count; i++ {
		_, err := c.Do("RPUSH", LIST, i)
		if err != nil {
			log.Panicf("fail to RPUSH %+v", err)
		}
		time.Sleep(time.Millisecond)
	}
	log.Printf("produce %d jobs", count)
}

func BConsume(buff *[]int) {
	c := pool.Get()
	defer c.Close()

	for {
		results, err := redis.Strings(c.Do("BLPOP", LIST, 0))
		if err != nil {
			log.Panic("fail to BLPOP %+v", err)
		}
		if len(results) > 1 {
			n, err := strconv.Atoi(results[1])
			if err != nil {
				log.Panicf("fail to Atoi %s, %+v", results[1], err)
			}
			if n > 0 {
				*buff = append(*buff, n)
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func Consume(buff *[]int) {
	c := pool.Get()
	defer c.Close()

	for {
		n, err := redis.Int(c.Do("LPOP", LIST))
		if err != nil && err != redis.ErrNil {
			log.Panic("fail to LPOP %+v", err)
		}
		if err == nil && n > 0 {
			*buff = append(*buff, n)
			time.Sleep(time.Millisecond)
		}
	}
}

func Monitor(total int, buff1 *[]int, buff2 *[]int) {
	c := pool.Get()
	defer func() {
		c.Close()
		wg.Done()
	}()
	for {
		size, err := redis.Int(c.Do("LLEN", LIST))
		if err != nil {
			log.Panicf("fail to LLEN %+v", err)
		}

		log.Printf("remains %d jobs", size)
		l1 := len(*buff1)
		l2 := len(*buff2)
		log.Printf("[worker#1] has finished %d jobs", l1)
		log.Printf("[worker#2] has finished %d jobs", l2)
		if l1+l2 == total {
			log.Printf("complete %d jobs", total)
			return
		}
		time.Sleep(time.Second * 2)
	}
}

var (
	pool *redis.Pool
	wg   sync.WaitGroup
)

const (
	LIST = "queue"
)

func init() {
	pool = util.NewRedisPool("127.0.0.1", 6379)
}

func main() {
	start := time.Now()
	buff1 := []int{}
	buff2 := []int{}
	wg.Add(1)

	n := 100000
	go Produce(n)

	go Consume(&buff1)

	//go BConsume(&buff2)

	go Monitor(n, &buff1, &buff2)

	wg.Wait()
	log.Printf("%s", time.Now().Sub(start))
}
