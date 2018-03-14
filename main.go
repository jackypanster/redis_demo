package main

import (
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jackypanster/util"
)

func Produce(count int) {
	c := pool.Get()
	defer c.Close()

	for i := 1; i <= count; i++ {
		if i % 2 == 0 {
			_, err := c.Do("RPUSH", LIST_B, i)
			if err != nil {
				log.Panicf("fail to RPUSH %+v", err)
			}
		} else {
			_, err := c.Do("RPUSH", LIST_A, i)
			if err != nil {
				log.Panicf("fail to RPUSH %+v", err)
			}
		}
	}
	log.Printf("produce %d jobs", count)
}

/*func BConsume(buff *[]int) {
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
}*/

func Consume(buff *[]int, list string) {
	c := pool.Get()
	defer c.Close()

	for {
		n, err := redis.Int(c.Do("LPOP", list))
		if err != nil && err != redis.ErrNil {
			log.Panic("fail to LPOP %+v", err)
		}
		if err == nil && n > 0 {
			*buff = append(*buff, n)
			time.Sleep(time.Millisecond * 1)
		}
	}
}

func Monitor(total int, buffs ...*[]int) {
	c := pool.Get()
	defer func() {
		c.Close()
		wg.Done()
	}()
	for {
		size, err := redis.Int(c.Do("LLEN", LIST_A))
		if err != nil {
			log.Panicf("fail to LLEN %+v", err)
		}

		log.Printf("remains %d jobs of A", size)

		size, err = redis.Int(c.Do("LLEN", LIST_B))
		if err != nil {
			log.Panicf("fail to LLEN %+v", err)
		}

		log.Printf("remains %d jobs of B", size)
		/*l1 := len(*buff1)
		l2 := len(*buff2)
		l3 := len(*buff3)
		log.Printf("[worker#1] has finished %d jobs", l1)
		log.Printf("[worker#2] has finished %d jobs", l2)
		log.Printf("[worker#3] has finished %d jobs", l3)*/
		sum := 0
		for idx, buff := range buffs {
			done := len(*buff)
			sum += done
			log.Printf("[worker#%d] has finished %d jobs", idx, done)
		}
		if sum == total {
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
	LIST_A = "queue_a"
	LIST_B = "queue_b"
)

func init() {
	pool = util.NewRedisPool("127.0.0.1", 6379)
}

func main() {
	start := time.Now()
	buff1 := []int{}
	buff2 := []int{}
	buff3 := []int{}
	buff4 := []int{}
	buff5 := []int{}
	buff6 := []int{}
	buff7 := []int{}
	buff8 := []int{}
	wg.Add(1)

	n := 1000000
	go Produce(n)

	go Consume(&buff1, LIST_A)

	go Consume(&buff2, LIST_A)

	go Consume(&buff3, LIST_A)

	go Consume(&buff4, LIST_A)

	go Consume(&buff5, LIST_B)

	go Consume(&buff6, LIST_B)

	go Consume(&buff7, LIST_B)

	go Consume(&buff8, LIST_B)

	go Monitor(n, &buff1, &buff2, &buff3, &buff4, &buff5, &buff6, &buff7, &buff8)

	wg.Wait()
	log.Printf("%s", time.Now().Sub(start))
}
