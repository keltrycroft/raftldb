# Build
```
go build
```
# Commands
SET key value 
 
GET key

# Building Cluster
```
./raftldb -n 1 -a :11001

./raftldb -n 2 -a :11002 -j :11001
./raftldb -n 3 -a :11003 -j :11001
```

# Using
```
redis-cli -p 11001
> SET cat tabby
"OK"
> GET cat
"tabby"
```

```
redis-cli -c -p 11002
redis-cli -c -p 11003
```

