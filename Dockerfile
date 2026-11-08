# syntax=docker/dockerfile:1
#
#   docker build -t mirror .                                    every service
#   docker build --build-arg SERVICES=@profiles/core.txt -t mirror-core .
#   docker build --build-arg SERVICES="aws.s3 aws.sqs" -t mirror-s3-sqs .
#
# SERVICES builds in only those services (scripts/slim-overlay.py): about 40%
# smaller, and a start measured in a fraction of a second instead of several.
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build
ARG TARGETOS TARGETARCH
ARG SERVICES=
WORKDIR /src
RUN apk add --no-cache python3
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# Cross-compiled on the build platform, so a multi-arch build needs no emulation.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; overlay=; \
    if [ -n "$SERVICES" ]; then \
      python3 -I scripts/slim-overlay.py /tmp/overlay.json $SERVICES; \
      overlay="-overlay /tmp/overlay.json"; \
    fi; \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
      go build -trimpath -buildvcs=false $overlay -ldflags '-s -w' -o /out/mirror ./cmd/mirror

FROM scratch
COPY --from=build /out/mirror /mirror
EXPOSE 4566
ENTRYPOINT ["/mirror"]
CMD ["up", "--bind", "0.0.0.0:4566"]
