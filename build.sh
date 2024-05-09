#!/bin/bash
par=$1

echo " " 
echo " >> Building..."

if [ "$par" == "clean" ]; then
    echo "Cleaning bin directory"
    rm -rf bin
    exit 0
fi

if [ "$par" == "all" ]; then
    os="linux"
    
    arch="amd64"
    echo "[$os $arch] Building http-server -> ./bin/$os-$arch/http-server"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/http-server cmd/http-server/main.go

    echo "[$os $arch] Building from-file -> ./bin/$os-$arch/from-file"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/from-file cmd/from-file/main.go

    echo "[$os $arch] Building fluent-out-parquet -> ./bin/$os-$arch/fluent-out-parquet.so"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$os-$arch/fluent-out-parquet.so cmd/fluent-out-parquet/main.go

    arch="arm64"
    echo "[$os $arch] Building http-server -> ./bin/$os-$arch/http-server"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -ldflags="-s -w" -o bin/$os-$arch/http-server cmd/http-server/main.go

    echo "[$os $arch] Building from-file -> ./bin/$os-$arch/from-file"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -ldflags="-s -w" -o bin/$os-$arch/from-file cmd/from-file/main.go

    echo "[$os $arch] Building fluent-out-parquet -> ./bin/$os-$arch/fluent-out-parquet.so"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -buildmode=c-shared -o bin/$os-$arch/fluent-out-parquet.so cmd/fluent-out-parquet/main.go

    exit 0    
fi

os=`go env GOOS`
arch=`go env GOARCH`

for d in cmd/* ; do
    echo "[$os $arch] Building ${d##*/} -> ./bin/$os-$arch/${d##*/}"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -o bin/$os-$arch/${d##*/} $d/main.go
done
