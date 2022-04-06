#pragma once
#include <cstring>
#include <thread>
#include <iostream>
#include <vector>
#include <future>
class Stats {
    std::vector<uint64_t> stats;
    std::string name;
public:
    Stats(): name("unnnamed"){}
    Stats(const std::string name_): name("repl_" + std::move(name_)) {}
    inline void add(uint64_t ns) {
        stats.push_back(ns);
    }
    ~Stats() {
        std::cout << name << ", ";
        if (stats.size() == 0) {
            std::cout << "weird...\n";
        }
        if (stats.size() == 1) {
            std::cout << stats.front() << "\n";
        } else {
            const auto sum = std::accumulate(stats.begin(), stats.end(), 0ULL);
            std::cout <<(int) ( ((double) sum) / stats.size() ) << "\n";
        }
        std::cout << std::flush;
    }
};

struct Clocker {
    const std::chrono::time_point<std::chrono::high_resolution_clock> start;
    Stats& stats;
    Clocker(Stats& stat): start(std::chrono::high_resolution_clock::now()), stats(stat){}
    inline uint64_t get_ns() const {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(high_resolution_clock::now() - start).count();
    }
    ~Clocker() {
        stats.add(get_ns());
    }
};

template <class C>
inline void run_test(std::string name, C& client, const std::vector<int>& read_addr,
              const std::vector<int>& write_addr, int rthread = 1,
              int wthread = 1, bool rw_seq = true)
{
    static std::string FONS = []() {
        char buf[4096];
        memset(buf, 'X', 4095);
        buf[sizeof(buf) - 1] = '\0';
        return std::string(buf, sizeof(buf));
    }();
    Stats st(name + ",total");
    Clocker _(st);
    auto read_fn = [&] (const int n_threads) {
        std::vector<std::thread> threads;
        Stats st(name + ",reads");
        Clocker _(st);
        for (int i = 0; i < n_threads; i++) {
            threads.emplace_back([&, i]() {
                const int n = read_addr.size();
                int st = (i * n) / n_threads;
                const int en = ((i + 1) * n) / n_threads;
                while (st != en) {
//                    std::cerr << "read @ " << read_addr[st] << "\n";
                    if (read_addr[st] < 0)
                        continue;
                    client.read(read_addr[st], FONS.length());
                    st++;
                }
            });
        }
        for (auto& t: threads) t.join();
        std::cerr << "all read thread done!\n";
    };
    auto write_fn = [&] (const int n_threads) {
        Stats st(name + ",writes");
        Clocker _(st);
        std::vector<std::thread> threads;
        for (int i = 0; i < n_threads; i++) {
            threads.emplace_back([&, i]() {
                const int n = write_addr.size();
                int st = (i * n) / n_threads;
                const int en = ((i + 1) * n) / n_threads;
                while (st != en) {
//                    std::cerr << "write @ " << write_addr[st] << "\n";
                    if (write_addr[st] < 0)
                        continue;
                    client.write(write_addr[st], FONS.length(), FONS.c_str());
//                    std::cerr << "\tdone @ " << write_addr[st] << "\n";
                    st++;
                }
            });
        }
        for (auto& t: threads) t.join();
        std::cerr << "all write thread done!\n";
    };
    std::future<void> f;
    if (rw_seq) {
        write_fn(wthread);
    } else {
        f = std::async(std::launch::async, [&]() { write_fn(wthread); });
    }
    read_fn(rthread);
}
