# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
ARG SERVICE=
RUN CGO_ENABLED=0 go build -o /out/mirror ./cmd/mirror

FROM scratch
COPY --from=build /out/mirror /mirror
EXPOSE 4566
ENTRYPOINT ["/mirror"]
CMD ["up", "--bind", "0.0.0.0:4566"]
