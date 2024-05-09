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
    archs=( "amd64" "arm64" )
    oses=( "linux" "windows" "darwin" )

    for os in "${oses[@]}"
    do
        for arch in "${archs[@]}"
        do
            echo "[$os $arch] Building http-server -> ./bin/$os-$arch/http-server"
            GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/http-server cmd/http-server/main.go

            echo "[$os $arch] Building from-file -> ./bin/$os-$arch/from-file"
            GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -ldflags="-s -w" -o bin/$os-$arch/from-file cmd/from-file/main.go

            echo "[$os $arch] Building fluent-out-parquet -> ./bin/$os-$arch/fluent-out-parquet.so"
            GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$os-$arch/fluent-out-parquet.so cmd/fluent-out-parquet/main.go
        done
    done
    exit 0    
fi

os=`go env GOOS`
arch=`go env GOARCH`

for d in cmd/* ; do
    echo "[$os $arch] Building ${d##*/} -> ./bin/$os-$arch/${d##*/}"
    GOOS=$os GOARCH=$arch CGO_ENABLED=1 go build -o bin/$os-$arch/${d##*/} $d/main.go
done
