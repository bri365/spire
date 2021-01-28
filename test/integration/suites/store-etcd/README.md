# Store etcd Suite

## Description

The suite runs the following etcd versions against the etcd store unit tests:

- 3.4.14

TODO: A special unit test binary is built from sources that targets the docker
containers running etcd.

## Running

```bash
SUITES='suites/store-etcd' make integration
```
