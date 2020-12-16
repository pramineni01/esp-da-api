build:
	cd cmd/da-api && go build
	cd cmd/da-api-internal && go build
	cd cmd/da-delete-ingests && go build
	cd cmd/da-process-erroneous-versions && go build
	cd cmd/da-rebuild-cache-layers && go build
	cd cmd/da-tools && go get github.com/gobuffalo/packr/v2/packr2 && packr2 && go build
fmt:
	cd cmd/da-api && go fmt
	cd cmd/da-api-internal && go fmt
	cd cmd/da-delete-ingests && go fmt
	cd cmd/da-rebuild-cache-layers && go fmt
generate:
	cd cmd/da-api && go run github.com/99designs/gqlgen generate
	cd cmd/da-api-internal && go run github.com/99designs/gqlgen generate
run:
	cd cmd/da-api && go run server.go
run-internal:
	cd cmd/da-api-internal && go run server.go
