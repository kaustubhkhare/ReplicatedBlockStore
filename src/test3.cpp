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

int FOUR_K = 4096;

int test3(int argc, char** argv) {
    auto client = GRPCClient::get_client(argc, argv);
    Stats st( "test3,aligned_writes");
    int offset = 4096, length = FOUR_K, iterations = 1000;
    std::string str(FOUR_K, 'k');
    {
        for (int i = 0; i < iterations; i++) {
            Clocker _(st);
            client->write(offset, length, str.c_str());
        }
    }

    return 0;
}

int test4(int argc, char** argv) {
    auto client = GRPCClient::get_client(argc, argv);
    Stats st( "test4,unaligned_writes");
    int offset = 1000, length = FOUR_K, iterations = 1000;
    std::string str(FOUR_K, 'k');
    {
        for (int i = 0; i < iterations; i++) {
            Clocker _(st);
            client->write(offset, length, str.c_str());
        }
    }

    return 0;
}

int test5(int argc, char** argv) {
    auto client = GRPCClient::get_client(argc, argv);
    Stats st( "test5,aligned_reads");
    int offset = 4096, length = FOUR_K, iterations = 1000;
    std::string t(FOUR_K, 'k');
    client->write(offset, length, t.c_str());
    {
        for (int i = 0; i < iterations; i++) {
            Clocker _(st);
            std::string str = client->read(offset, length);
        }
    }

    return 0;
}

int test6(int argc, char** argv) {
    auto client = GRPCClient::get_client(argc, argv);
    Stats st( "test6,unaligned_reads");
    int offset = 1000, length = FOUR_K, iterations = 1000;
    std::string t(FOUR_K, 'k');
    client->write(offset, length, t.c_str());
    {
        for (int i = 0; i < iterations; i++) {
            Clocker _(st);
            std::string str = client->read(offset, length);
        }
    }

    return 0;
}

int main(int argc, char *argv[]) {
    if (!strcmp("3", argv[1]))
        test3(argc, argv);
    else if (!strcmp("4", argv[1]))
        test4(argc, argv);
    else if (!strcmp("5", argv[1]))
        test5(argc, argv);
    else if (!strcmp("6", argv[1]))
        test6(argc, argv);
    return 0;
}