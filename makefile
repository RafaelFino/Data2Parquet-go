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

full:
	@./build.sh clean
	@./build.sh lint
	@./build.sh test
	@./build.sh all