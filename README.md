# Simple Goroutine Pool

## Usage
```
pool := gpool.NewPool(5)
defer pool.Close()
f := func(v interface{}) { fmt.Println(v) }
data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
pool.Map(f, data)
pool.SetMaxWorker(2)
pool.Map(f, data)
```