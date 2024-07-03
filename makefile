build:
	@./build.sh current

linux:
	@./build.sh linux

clean:
	@./build.sh clean

lint:
	@./build.sh lint

test:
	@./build.sh test

container:
	@./docker-build.sh

all:
	@./build.sh clean
	@./build.sh lint
	@./build.sh test
	@./build.sh linux