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
	Addr:"172.23.0.4:6379",
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

func TestList(t *testing.T) {
	client.RPush(ctx,"names","Rama")
	client.RPush(ctx,"names","Fajar")

	assert.Equal(t, "Rama", client.LPop(ctx,"names").Val())
	assert.Equal(t,"Fajar",client.LPop(ctx,"names").Val())

	client.Del(ctx,"names")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "students", "Rama")
	client.SAdd(ctx, "students", "Rama")
	client.SAdd(ctx, "students", "Fajar")
	client.SAdd(ctx, "students", "Fajar")

	assert.Equal(t, int64(2), client.SCard(ctx,"students").Val())
	assert.Equal(t, []string{"Rama","Fajar"}, client.SMembers(ctx,"students").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score:100, Member:"Fajar"})
	client.ZAdd(ctx, "scores", redis.Z{Score:85, Member:"Feast"})
	client.ZAdd(ctx, "scores", redis.Z{Score:65, Member:"Rama"})
	client.ZAdd(ctx, "scores", redis.Z{Score:95, Member:"Hindia"})
	client.ZAdd(ctx, "scores", redis.Z{Score:80, Member:"Bernadya"})

	assert.Equal(t,[]string{"Rama","Bernadya","Feast","Hindia","Fajar"}, client.ZRange(ctx,"scores", 0,4).Val())
	assert.Equal(t,"Fajar",client.ZPopMax(ctx,"scores").Val()[0].Member)
	assert.Equal(t,"Hindia",client.ZPopMax(ctx,"scores").Val()[0].Member)
	assert.Equal(t,"Feast",client.ZPopMax(ctx,"scores").Val()[0].Member)
	assert.Equal(t,"Bernadya",client.ZPopMax(ctx,"scores").Val()[0].Member)
	assert.Equal(t,"Rama",client.ZPopMax(ctx,"scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1","id","1")
	client.HSet(ctx, "user:1","name","Fajar")
	client.HSet(ctx, "user:1","email","fajar@example.com")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Fajar", user["name"])
	assert.Equal(t,"fajar@example.com",user["email"])

	client.Del(ctx,"user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx,"sellers", &redis.GeoLocation{
		Name:"Toko A",
		Longitude:106.818489,
		Latitude:-6.178966,
	})

	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:"Toko B",
		Longitude:106.821568,
		Latitude:-6.180662,
	})

	distance := client.GeoDist(ctx, "sellers","Toko A","Toko B","km").Val()
	assert.Equal(t,0.3892,distance)

	client.GeoSearch(ctx,"sellers", &redis.GeoSearchQuery{
		Longitude:0,
		Latitude:0,
		Radius:5,
		RadiusUnit:"km",
	})
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors","Rama","Fajar")
	client.PFAdd(ctx, "visitors","Purbaya","Susi")
	client.PFAdd(ctx, "visitors","Freya","Flora")
	assert.Equal(t,int64(6), client.PFCount(ctx,"visitors").Val())
}

func TestPipeline(t *testing.T) {
	_, err := client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name","Fajar", 5 * time.Second)
		pipeliner.SetEx(ctx, "address","Indonesia", 5 * time.Second)
		return nil
	})

	assert.Nil(t, err)
}

func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name","Fajar", 5 * time.Second)
		pipeliner.SetEx(ctx, "address","Indonesia", 5 * time.Second)
		return nil
	})

	assert.Nil(t, err)

	assert.Equal(t, "Fajar", client.Get(ctx,"name").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx,"address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i ++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream:"members",
			Values:map[string]interface{}{
				"name":"Fajar",
				"address":"Indonesia",
			},
		}).Err()

		assert.Nil(t, err)
	}
}