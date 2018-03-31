package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jackypanster/util"
)

var (
	pool *redis.Pool
	wg   sync.WaitGroup
)

func init() {
	pool = util.NewRedisPool("127.0.0.1", 6379)
}

func produce(total int) {
	c := pool.Get()
	defer c.Close()
	defer wg.Done()
	for i := 1; i <= total; i++ {
		_, err := c.Do("RPUSH", "Jobs", i)
		if err != nil {
			log.Panicf("fail to push %s", err.Error())
		}
		fmt.Println(i)
		time.Sleep(time.Millisecond)
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
	fmt.Println(num)

	cpu := runtime.NumCPU()
	fmt.Println(cpu)
	wg.Add(cpu)
	for i := 0; i < cpu; i++ {
		go produce(num / cpu)
	}

	wg.Wait()
}
