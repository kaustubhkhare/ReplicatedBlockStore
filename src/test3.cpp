#include <iostream>
#include <vector>
#include "client.h"

class Stats {
    std::vector<uint64_t> stats;
    std::string name;
public:
    Stats(): name("unnnamed"){}
    Stats(const std::string name_): name("RepBlockStore" + std::move(name_)) {}
    void add(uint64_t ns) {
        stats.push_back(ns);
    }
    ~Stats() {
        std::cout << name << ",";
        if (stats.size() == 0) {
            std::cout << "weird...\n";
        }
        if (stats.size() == 1) {
            std::cout << stats.front() << "\n";
        } else {
            const auto sum = std::accumulate(stats.begin(), stats.end(), 0ULL);
            std::cout << "first," << stats.front() << "\n";
            std::cout << name << "," << "average,";
            std::cout <<(int) ( ((double) sum) / stats.size() ) << "\n";
        }
    }
};

struct Clocker {
    const std::chrono::time_point<std::chrono::high_resolution_clock> start;
    Stats& stats;
    Clocker(Stats& stat): start(std::chrono::high_resolution_clock::now()), stats(stat){}
    uint64_t get_ns() const {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(high_resolution_clock::now() - start).count();
    }
    ~Clocker() {
        stats.add(get_ns());
    }
};

int test3(int argc, char** argv) {
    auto client = GRPCClient::get_client(argc, argv);
    Stats st( "test3,writes");
    int offset = 1000, length = 4096, iterations = 1000;
    std::string str(4096, 'k');
    {
        for (int i = 0; i < iterations; i++) {
            Clocker _(st);
            client->write(offset, length, str.c_str());
        }
    }

    return 0;
}

int main(int argc, char *argv[]) {
    return test3(argc, argv);
}