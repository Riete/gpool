# Simple Goroutine Pool

## Usage

### Pool
```
f1 := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println("f1 ", v) }
f2 := func(v interface{}) { time.Sleep(2 * time.Second); fmt.Println("f2 ", v) }
pool := NewPool(6)

defer pool.Close()
pool.Run()
data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
for _, d := range data {
    pool.Submit(f1, d)
    pool.Submit(f2, d)
}
pool.Wait()
```

### PoolWithFunc
```
f := func(v interface{}) { time.Sleep(1 * time.Second); fmt.Println(v) }
pool := NewPoolWithFunc(f, 2)
defer pool.Close()
data := []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
data2 := []interface{}{"aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn"}
pool.Run(data)
pool.SetMaxWorker(4)
pool.Run(data2)
```