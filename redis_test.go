package belajar_golang_redis

import (
	"github.com/redis/go-redis/v9"
	"testing"
	"github.com/stretchr/testify/assert"
	"context"
	"fmt"
	"time"
)

var client=redis.NewClient(&redis.Options{
	Addr:"172.23.0.3:6379",
	DB:0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)

	// assert.Nil(t,err)
}

var ctx =context.Background()
func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	fmt.Println(result)
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx,"name","Rama Fajar Fadhillah", time.Second * 3)

	result, err := client.Get(ctx,"name").Result()
	assert.Nil(t,err)
	assert.Equal(t, "Rama Fajar Fadhillah", result)

	time.Sleep(time.Second * 5)
	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}