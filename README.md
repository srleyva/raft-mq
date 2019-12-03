# Start the thingie!

```
$ go run main.go --nodes localhost:12000,localhost:13000,localhost:14000,localhost:15000 --port 12000 --node-id 1 --cluster-count 4 /tmp/raft/ &
$ go run main.go --nodes localhost:12000,localhost:13000,localhost:14000,localhost:15000 --port 13000 --node-id 2 --cluster-count 4 /tmp/raft/ &
$ go run main.go --nodes localhost:12000,localhost:13000,localhost:14000,localhost:15000 --port 14000 --node-id 3 --cluster-count 4 /tmp/raft/ &
$ go run main.go --nodes localhost:12000,localhost:13000,localhost:14000,localhost:15000 --port 15000 --node-id 4 --cluster-count 4 /tmp/raft/ &
```

# Start the test

```
$ go run client/client.go
```