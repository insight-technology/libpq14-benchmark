bench: bench.cpp
	g++ -O2 bench.cpp -std=c++11 -I./libpq/include -Wl,-rpath ./libpq/lib -L./libpq/lib -lpq -o bench
