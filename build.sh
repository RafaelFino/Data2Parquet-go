#!/bin/bash
par=$1

if [ "$par" == "clean" ]; then
    echo ">> Cleaning bin directory"
    rm -rf bin
    exit 0
fi

if [ "$par" == "lint" ]; then
    echo ">> Linting..."
    docker run -t --rm -v $(pwd):/app -w /app golangci/golangci-lint:v1.58.1 golangci-lint run -v
    exit 0
fi

if [ "$par" == "test" ]; then
    echo ">> Testing..."
    go test ./...
    exit 0
fi

if [ "$par" == "all" ]; then
    os="linux"
    arch="amd64"

    echo ">> Building for $os $arch"
    echo ">>   [$os $arch] Building http-server -> ./bin/$os-$arch/http-server"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/http-server -ldflags="-s -w" -trimpath cmd/http-server/main.go

    echo ">>   [$os $arch] Building json2parquet -> ./bin/$os-$arch/json2parquet"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/json2parquet -ldflags="-s -w" -trimpath cmd/json2parquet/main.go

    echo ">>   [$os $arch] Building data-generator -> ./bin/$os-$arch/data-generator"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/data-generator -ldflags="-s -w" -trimpath cmd/data-generator/main.go

    echo ">>   [$os $arch] Building fluent-out-parquet -> ./bin/$os-$arch/fluent-out-parquet.so"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$os-$arch/fluent-out-parquet.so -ldflags="-s -w" -trimpath cmd/fluent-out-parquet/main.go

    arch="arm64"
    echo ">> Building for $os $arch"
    echo ">>   [$os $arch] Building http-server -> ./bin/$os-$arch/http-server"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -ldflags="-s -w" -o bin/$os-$arch/http-server -ldflags="-s -w" -trimpath cmd/http-server/main.go

    echo ">>   [$os $arch] Building json2parquet -> ./bin/$os-$arch/json2parquet"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -ldflags="-s -w" -o bin/$os-$arch/json2parquet -ldflags="-s -w" -trimpath cmd/json2parquet/main.go

    echo ">>   [$os $arch] Building data-generator -> ./bin/$os-$arch/data-generator"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -ldflags="-s -w" -o bin/$os-$arch/data-generator -ldflags="-s -w" -trimpath cmd/data-generator/main.go    

    echo ">>   [$os $arch] Building fluent-out-parquet -> ./bin/$os-$arch/fluent-out-parquet.so"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -buildmode=c-shared -o bin/$os-$arch/fluent-out-parquet.so -ldflags="-s -w" -trimpath cmd/fluent-out-parquet/main.go

    exit 0    
fi

if [ "$par" == "current" ]; then
    os=`go env GOOS`
    arch=`go env GOARCH`

    echo ">> Building for $os $arch"
    echo ">>   [$os $arch] Building http-server -> ./bin/$os-$arch/http-server"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/http-server -ldflags="-s -w" -trimpath cmd/http-server/main.go

    echo ">>   [$os $arch] Building json2parquet -> ./bin/$os-$arch/json2parquet"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/json2parquet -ldflags="-s -w" -trimpath cmd/json2parquet/main.go

    echo ">>   [$os $arch] Building data-generator -> ./bin/$os-$arch/data-generator"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/data-generator -ldflags="-s -w" -trimpath cmd/data-generator/main.go

    echo ">>   [$os $arch] Building fluent-out-parquet -> ./bin/$os-$arch/fluent-out-parquet.so"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$os-$arch/fluent-out-parquet.so -ldflags="-s -w" -trimpath cmd/fluent-out-parquet/main.go

    exit 0
fi

echo ">> Usage: ./build.sh [clean|lint|test|all|current]"