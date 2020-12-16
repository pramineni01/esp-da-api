FROM golang:latest as builder

WORKDIR /esp-da-api
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp-da-api cmd/da-api/server.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp-da-api-internal cmd/da-api-internal/server.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp-da-rebuild-cache-layers cmd/da-rebuild-cache-layers/rebuild_cache.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp-da-delete-ingests cmd/da-delete-ingests/delete_ingests.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp-da-process-erroneous-versions cmd/da-process-erroneous-versions/process_versions.go
RUN cd cmd/da-tools/
RUN CGO_ENABLED=0 GOOS=linux go get github.com/gobuffalo/packr/v2/packr2 
RUN packr2
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o esp-da-tools main.go

FROM alpine:latest

WORKDIR /opt/esp-da-api/
COPY --from=builder /esp-da-api/esp-da-api external-api
COPY --from=builder /esp-da-api/esp-da-api-internal internal-api
COPY --from=builder /esp-da-api/esp-da-tools da-tools
COPY --from=builder /esp-da-api/esp-da-rebuild-cache-layers da-rebuild-cache-layers
COPY --from=builder /esp-da-api/esp-da-delete-ingests da-delete-ingests
COPY --from=builder /esp-da-api/esp-da-process-erroneous-versions da-process-erroneous-versions
COPY ./config ./config

ENTRYPOINT ["./external-api"]