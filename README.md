# gonsensus

[![Go Reference](https://pkg.go.dev/badge/github.com/thevilledev/gonsensus.svg)](https://pkg.go.dev/github.com/thevilledev/gonsensus)
[![Go Report Card](https://goreportcard.com/badge/github.com/thevilledev/gonsensus)](https://goreportcard.com/report/github.com/thevilledev/gonsensus)

"Gonsensus" is a distributed consensus implementation, written in Go, using S3 conditional operations for leader election. It provides a simple, reliable way to coordinate distributed systems using *any* S3 compatible object storage as the backing store. Conditional writes are required. See [RFC 7232](https://datatracker.ietf.org/doc/html/rfc7232) for more details about conditional requests in general.

See [example_test.go](example_test.go) for a fully fledged example use case.

## Features

- Leader election using S3 atomic operations
- Automatic leader failover
- Configurable TTL and poll intervals
- Callback hooks for leader election and demotion
- Clean shutdown handling
- Thread-safe operations
- No external dependencies beyond AWS SDK

## Installation

```bash
go get github.com/thevilledev/gonsensus
```

## Getting started

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/yourusername/gonsensus"
)

func main() {
    // Load AWS config
    cfg, err := config.LoadDefaultConfig(context.Background())
    if err != nil {
        log.Fatal(err)
    }

    // Create consensus manager
    manager, err := gonsensus.NewManager(
        s3.NewFromConfig(cfg),
        "your-bucket-name",
        gonsensus.Config{
            TTL:          30 * time.Second,
            PollInterval: 5 * time.Second,
            LockPrefix:   "consensus/",
            NodeID:       "node-1",
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    // Set callbacks
    manager.SetCallbacks(
        func(ctx context.Context) error {
            log.Println("Elected as leader!")
            return nil
        },
        func(ctx context.Context) {
            log.Println("Demoted from leader")
        },
    )

    // Run the consensus manager
    if err := manager.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

## How it works

Gonsensus uses atomic operations to implement a distributed lock mechanism:

### Lock Acquisition

- Nodes attempt to create a lock file in S3
- Only one node can hold the lock at a time
- Lock includes expiry time and version information

### Leader Maintenance

- Leader renews lock every TTL/3 period
- Uses versioning to ensure atomic updates
- Automatic failover if leader fails to renew

### Lock Release

- Automatic on process termination
- Clean shutdown on signals
- Other nodes can take over after TTL expiry

## License

This project is licensed under the MIT License - see the LICENSE file for details.