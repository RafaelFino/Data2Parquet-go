build:
	@./build.sh current

all:
	@./build.sh clean
	@./build.sh lint
	@./build.sh test
	@./build.sh all

clean:
	@./build.sh clean

lint:
	@./build.sh lint

test:
	@./build.sh test