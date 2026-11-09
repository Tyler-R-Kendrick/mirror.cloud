# Using mirror

How to get mirror, run it, point a client at it, and keep it small. For what each service does, see [SUPPORT.md](SUPPORT.md); for how a release is made, [RELEASING.md](RELEASING.md).

## Get it

Pick one. All of them are the same program.

| You want | Get | Download | On disk |
|---|---|---|---|
| every service | `mirror_<tag>_<os>_<arch>` archive, or image `mirror:<version>` | 14 MB | 30 MB |
| the 12 services most test suites use ([`profiles/core.txt`](../profiles/core.txt)) | `…_core` archive, or image `mirror:<version>-core` | 8 MB | 21 MB |
| exactly the services you name | build it yourself, below | smaller still | smaller still |

Sizes are linux/amd64. The core build also starts in under a second and uses about 60 MB of memory, against about 5 seconds and 415 MB for the full one, because it does not decode the models of services you will not call. If you do not know that you need the rest, take core.

### A release archive

Archives are on the project's GitHub Releases page, with a `SHA256SUMS` file. Check, unpack, run:

```sh
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf mirror_v0.0.1-alpha.1_linux_amd64_core.tar.gz    # .zip on Windows
./mirror up
```

The archive holds `LICENSE` and one file, `mirror` (`mirror.exe` on Windows). There is no installer and no daemon. `mirror version` prints the release without its `v`.

### A container image

```sh
docker run --rm -p 127.0.0.1:4566:4566 ghcr.io/tyler-r-kendrick/mirror:0.0.1-alpha.1-core
```

| Tag | Holds |
|---|---|
| `0.0.1-alpha.1` | every service, that release |
| `0.0.1-alpha.1-core` | the core services, that release |
| `latest`, `core` | the newest release that is not a prerelease; none exists until there is one |

A release whose tag has a hyphen (`v0.0.1-alpha.1`) is a prerelease and gets only its version tags, so `latest` does not move until there is a release without one. Pin a version in anything you keep. Both images are multi-arch (linux/amd64, linux/arm64), contain nothing but the `mirror` binary, and have no shell. They run `mirror up --bind 0.0.0.0:4566` unless you give a command, and then you give the whole command:

```sh
docker run --rm -p 127.0.0.1:4566:4566 ghcr.io/tyler-r-kendrick/mirror:0.0.1-alpha.1-core up s3 sqs --bind 0.0.0.0:4566
```

`-p 127.0.0.1:4566:4566` keeps the port on your machine. **mirror has no authentication**: `-p 4566:4566` publishes it on every interface of the host. In the container it listens on `0.0.0.0` and prints a warning to that effect at every start; the port mapping is what keeps it private.

The binary in an image is the binary in the archive of the same release; to check, copy it out and compare:

```sh
id=$(docker create ghcr.io/tyler-r-kendrick/mirror:0.0.1-alpha.1-core)
docker cp "$id":/mirror ./mirror && docker rm "$id"
sha256sum mirror; tar -xzOf mirror_v0.0.1-alpha.1_linux_amd64_core.tar.gz mirror | sha256sum
```

### Exactly the services you name

Build a binary, or an image, from only the services you list. Nothing in the source changes; the build leaves the rest out.

```sh
make build-slim SERVICES="aws.s3 aws.sqs aws.dynamodb"     # bin/mirror-slim
make build-slim SERVICES=@my-services.txt                  # one ID per line, # for comments
docker build --build-arg SERVICES="aws.s3 aws.sqs" -t mirror-mine .
```

IDs are as `mirror services` prints them. A name that is not a service is refused. A call from a service you kept into one you left out (S3 notifying a queue, SNS invoking a function) fails at that call and names the missing service. A binary built this way is the one place to look at what it serves: see "What is running" below.

## Run it

```sh
mirror up                      # every service the binary has, on 127.0.0.1:4566
mirror up s3 sqs               # only these
mirror up --strict             # refuse mock-tier operations instead of faking them
```

It prints `mirror <version> listening on http://127.0.0.1:4566` once the port is bound, and exits 1 with just the error if it cannot bind. Stop it with Ctrl-C or SIGTERM (`docker stop`); it finishes the requests in flight (for at most five seconds), writes `--persist` state if you asked for it, and exits 0.

### What is running

```sh
curl -fsS http://127.0.0.1:4566/_mirror/health      # {"status":"ok",...}
curl -fsS http://127.0.0.1:4566/_mirror/services    # what is actually served, with tier and operation count
mirror services                                     # the same, for the binary you have, without starting it
```

`/_mirror/health` is the readiness check. `up` names whatever you asked for in its `services:` line, **including a service the binary does not contain**: `mirror up ec2` on a core build listens and serves nothing, and `/_mirror/services` says `null`. Trust the endpoint, not the line.

There is no shell in the image, so a `HEALTHCHECK` that runs `curl` inside it cannot work. Check from outside.

## Point a client at it

Any credentials work, and so do none that a client needs to send. `mirror env` prints what to export:

```sh
eval "$(mirror env)"
# AWS_ENDPOINT_URL=http://127.0.0.1:4566   AWS_ENDPOINT_URL_S3=…   AWS_ACCESS_KEY_ID=test
# AWS_SECRET_ACCESS_KEY=test   AWS_DEFAULT_REGION=us-east-1   AWS_S3_FORCE_PATH_STYLE=true
# STORAGE_EMULATOR_HOST=…   VERCEL_API_URL=…   VERCEL_TOKEN=test   AWS_EC2_METADATA_DISABLED=true
```

Give `mirror env` the same `--bind`, `--tls-cert` or `--tls-self-signed` as `mirror up` and the endpoints match. `mirror doctor` says what in your environment would stop a client reaching it. From a container, set the same variables yourself with the host or service name in the URL.

## Configure it

Flags, then environment, then `.mirror/config.json` in the working directory (which is `/` in the image), then the defaults below; the first one given wins.

| Flag | Environment | `config.json` | Default | |
|---|---|---|---|---|
| `--bind ADDR` | `MIRROR_BIND` | `bind` | `127.0.0.1:4566` | listen address |
| `--advertise-url URL` | `MIRROR_ADVERTISE_URL` | `advertise_url` | the bind address | the address mirror puts in URLs it returns (SQS queue URLs, for one); set it to what other containers call it |
| `--seed STRING` | `MIRROR_SEED` | `seed` | `mirror` | seeds every generated ID and timestamp, so a run repeats |
| `--persist DIR` | `MIRROR_PERSIST` | `persist` | none | load state at start, save it at a clean stop |
| `--strict` | `MIRROR_STRICT` | `strict` | off | refuse mock-tier operations |
| `--sqs-endpoint-strategy` | `MIRROR_SQS_ENDPOINT_STRATEGY` | `sqs_endpoint_strategy` | `off` | `off`, `standard`, `domain` or `path` |
| | `MIRROR_DEFAULT_REGION` | `region` | `us-east-1` | |
| | `MIRROR_DEFAULT_ACCOUNT` | `account` | `000000000000` | |
| | `MIRROR_S3_VALIDATE_PRESIGNED_SIGNATURES` | `s3_validate_presigned_signatures` | off | verify S3 signatures instead of parsing them |
| | `MIRROR_S3_ALLOW_NONSTANDARD_REGIONS` | `s3_allow_nonstandard_regions` | off | accept an S3 bucket region AWS does not have |
| | `MIRROR_VERCEL_FAULTS`, `MIRROR_VERCEL_STRICT_AUTH` | `vercel_faults`, `vercel_strict_auth` | off | the Vercel emulation's failure modes, [VERCEL.md](VERCEL.md) |

`--tier ID=mock|emulate|proxy` sets one service's tier, and `--tls-cert`/`--tls-key` or `--tls-self-signed DIR` serve HTTPS; the [README](../README.md) has the TLS details. `mirror help` lists every flag.

Boolean variables are on for `1` or `true`.

## Keep state between runs

```sh
mirror up --persist ./state          # state.tar in ./state, read at start, written at a clean stop
```

State is written when mirror is stopped by Ctrl-C or SIGTERM, not when it is killed (`kill -9`, an out-of-memory kill, a `docker kill`): that loses what happened since the start. In a container, mount a volume and use the default stop:

```sh
docker run --rm -p 127.0.0.1:4566:4566 -v mirror-state:/data \
  ghcr.io/tyler-r-kendrick/mirror:0.0.1-alpha.1-core up --bind 0.0.0.0:4566 --persist /data
```

`docker stop` waits ten seconds before it kills, which is longer than the five mirror needs at most.

## In CI

A GitHub Actions service container, waiting on the health check:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      mirror:
        image: ghcr.io/tyler-r-kendrick/mirror:0.0.1-alpha.1-core
        ports: ["4566:4566"]
    env:
      AWS_ENDPOINT_URL: http://localhost:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_DEFAULT_REGION: us-east-1
      AWS_S3_FORCE_PATH_STYLE: "true"
    steps:
      - uses: actions/checkout@v4
      - run: curl -fsS --retry 30 --retry-connrefused --retry-delay 1 http://localhost:4566/_mirror/health
      - run: ./run-your-tests
```

Or without a container, from an archive: download it, `sha256sum --check`, `./mirror up &`, and wait on the same URL.

With Docker Compose, other containers reach it by service name, so tell it that name:

```yaml
services:
  mirror:
    image: ghcr.io/tyler-r-kendrick/mirror:0.0.1-alpha.1-core
    command: ["up", "--bind", "0.0.0.0:4566", "--advertise-url", "http://mirror:4566"]
    ports: ["127.0.0.1:4566:4566"]
  app:
    environment:
      AWS_ENDPOINT_URL: http://mirror:4566
    depends_on: [mirror]
```

`depends_on` here waits for the container to start, not to be ready; a client that retries its first connection, or a wait on `/_mirror/health`, covers the second.

## If a pull is refused

The packages of `v0.0.1-alpha.1` are public and need no login. If a later package is private, `docker pull` needs `docker login ghcr.io` with a token that can read packages, and a GitHub Actions service container needs `credentials:` in the workflow.
