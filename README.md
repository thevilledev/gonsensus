# gonsensus

[![Go Reference](https://pkg.go.dev/badge/github.com/thevilledev/gonsensus.svg)](https://pkg.go.dev/github.com/thevilledev/gonsensus)
[![test](https://github.com/thevilledev/gonsensus/actions/workflows/test.yml/badge.svg)](https://github.com/thevilledev/gonsensus/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/thevilledev/gonsensus)](https://goreportcard.com/report/github.com/thevilledev/gonsensus)

**Gonsensus** is a distributed consensus implementation, written in Go, using S3 conditional operations for leader election. It provides a simple, reliable way to coordinate distributed systems using *any* S3 compatible object storage as the backing store. Conditional writes are required. See [RFC 7232](https://datatracker.ietf.org/doc/html/rfc7232) for more details about conditional requests in general.

See [example_test.go](example_test.go) for a fully fledged example use case for simple leader election.

See [integration_test.go](integration/integration_test.go) for an example use case for leader election with quorum.

## Features

- S3-based leader election with atomic operations
- Automatic failover and clean shutdown
- Configurable TTL, polling, and quorum settings
- Leader election and demotion callbacks
- Observer pattern with heartbeat monitoring
- Thread-safe operations with mutex protection
- Version control for consistency
- Minimal dependencies (AWS SDK only)

## How it works

Gonsensus implements distributed consensus through S3:

### Leader Election
- Nodes compete to create a lock file in S3 using atomic operations
- Winner becomes leader and maintains lock through periodic renewals
- Failed renewals trigger automatic failover

### Optional Quorum Management
- When enabled, leader tracks observer nodes through heartbeats
- Configurable quorum size ensures consensus
- Can operate in simple leader-election mode without quorum

### Fault Tolerance
- Automatic failover on leader failure
- Version control prevents split-brain scenarios
- Grace periods handle clean transitions
## Installation

```bash
go get github.com/thevilledev/gonsensus
```

## Getting started

```go
package main

import (
    "context"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/thevilledev/gonsensus"
)

func main() {
    // Initialize AWS S3 client
    cfg, _ := config.LoadDefaultConfig(context.Background())
    s3Client := s3.NewFromConfig(cfg)

    // Create consensus manager
    manager, _ := gonsensus.NewManager(s3Client, "my-bucket", gonsensus.Config{
        NodeID:     "node-1",
        LockPrefix: "consensus/",
    })

    // Set leader callbacks
    manager.SetCallbacks(
        func(ctx context.Context) error { /* on elected */ return nil },
        func(ctx context.Context) { /* on demoted */ },
    )

    // Run manager
    manager.Run(context.Background())
}
```

## Authentication

This project relies on the AWS S3 client default authentication mechanism. See AWS documentation for full reference on [configuration and credentials presedence](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#configure-precedence).

In short, if you want to use environment variables you may do it like this:

```bash
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_ENDPOINT_URL_S3=
export AWS_ENDPOINT_URL_IAM=
export AWS_ENDPOINT_URL_STS=
export AWS_REGION=

# run acceptance tests
make test-acc
```

If your configuration is in a local AWS profile in `~/.aws/config` you may do it like this instead:

```bash
export AWS_PROFILE=xyz

# run acceptance tests
make test-acc
```

Note that acceptance tests require a bucket with the name of "my-bucket-1".

## License

This project is licensed under the MIT License - see the [LICENSE file](LICENSE) for details.