GO=CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go
BIN=worker_cleaner
IMAGE=quay.io/molecule/$(BIN)
IMAGE_TAG=$(IMAGE):$(DOCKER_TAG)

all: image
	docker push $(IMAGE_TAG)

setup:
	glide install

build:
	$(GO) build -a -installsuffix cgo -o $(BIN) .

image: build
	docker build -t $(IMAGE_TAG) .

.PHONY: clean

clean:
	rm $(BIN)
