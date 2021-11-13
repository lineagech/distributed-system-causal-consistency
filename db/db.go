package db

import (
    "fmt"
    "errors"
    "context"
    "github.com/go-redis/redis/v8"
)

var ctx = context.Background()

var rdb *Client = nil

func ExampleClient() {
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:7001",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

    err := rdb.Set(ctx, "key", "value-chia-hao", 0).Err()
    if err != nil {
        panic(err)
    }

    val, err := rdb.Get(ctx, "key").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println("key", val)

    val2, err := rdb.Get(ctx, "key2").Result()
    if err == redis.Nil {
        fmt.Println("key2 does not exist")
    } else if err != nil {
        panic(err)
    } else {
        fmt.Println("key2", val2)
    }
    // Output: key value
    // key2 does not exist
}

func InitDB() error {
    rdb = redis.NewClient(&redis.Options{
        Addr:     "localhost:7001",
        Password: "",
        DB:       0,
    })
    if rdb == nil {
        panic(err)
        return errors.New("Initialize Redis DB failed")
    }
    return nil
}

func DB_Set(key string, value string) error {
    err := rdb.Set(ctx, key, value, 0).Err()
    if err != nil {
        panic(err)
        return errors.New(fmt.Sprintf("DB Set (%s,%s) failed"))
    }
    return nil
}

func DB_Get(key string) string {
    value, err := rdb.Get(ctx, key).Result()
    if err == redis.Nil {
        fmt.Printf("Key %s does not exist\n", key)
    } else if err != nil {
        panic(err)
        return errors.New(fmt.Sprintf("DB Get (%s) failed", key))
    } else {
        fmt.Println("DB Get (key %s: value %s)", key, value)
    }
    return value
}

//func main() {
//    ExampleClient()
//}
