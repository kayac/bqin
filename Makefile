GIT_VER := $(shell git describe --tags)
DATE := $(shell date +%Y-%m-%dT%H:%M:%S%z)
export GO111MODULE := on

.PHONY: build test install clean

cmd/bqin/bqin: bqin.go processer.go receiver.go config.go cmd/bqin
	cd cmd/bqin && go build -ldflags "-s -w -X main.version=${GIT_VER} -X main.buildDate=${DATE}"

install: cmd/bqin/bqin
	go install cmd/bqin

test:
	go test -v ./...

packages: bqin.go processer.go receiver.go config.go cmd/bqin
	cd cmd/bqin && gox -os="linux darwin" -arch="amd64" -output "../../pkg/{{.Dir}}-${GIT_VER}-{{.OS}}-{{.Arch}}" -ldflags "-s -w -X main.version=${GIT_VER} -X main.buildDate=${DATE}"
	cd pkg && find . -name "*${GIT_VER}*" -type f -exec zip {}.zip {} \;

release:
	ghr -u kayac -r bqin -n "$(GIT_VER)" $(GIT_VER) pkg/

clean:
	rm -f cmd/bqin/bqin pkg/*
