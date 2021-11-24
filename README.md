# Simple Goroutine Pool

## Usage

### pool.Map
```
pool := gpool.NewPool(5)
defer pool.Close()
f := func(v interface{}) { fmt.Println(v) }
data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
pool.Map(f, data)
pool.SetMaxWorker(2)
pool.Map(f, data)
```

### pool.Submit
```
pool := gpool.NewPool(5)
defer pool.Close()
f1 := func(v interface{}) { fmt.Println("f1", v) }
f2 := func(v interface{}) { fmt.Println("f2", v) }
data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
fs := []func(interface{}){f1, f2, f1, f2, f1, f2, f1, f2, f1, f2, f1, f2, f1, f2}
for i := len(data) - 1; i >= 0; i-- {
    pool.Submit(fs[i], data[i])
}
pool.Wait()
```