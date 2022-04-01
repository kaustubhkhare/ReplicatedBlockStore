#include <thread>
#include <iostream>
#include <vector>
#include <future>
class Stats {
    std::vector<uint64_t> stats;
    std::string name;
public:
    Stats(): name("unnnamed"){}
    Stats(const std::string name_): name("thrift_" + std::move(name_)) {}
    void add(uint64_t ns) {
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

template <class C>
void run_test(std::string name, C& client, const std::vector<int>& read_addr,
              const std::vector<int>& write_addr, int rthread = 1,
              int wthread = 1, bool rw_seq = true)
{
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
                    client.read(read_addr[st]);
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
                    client.write(write_addr[st], 4, "AAA");
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
        f = std::async(std::launch::async, [&]() { write_fn(rthread); });
    }
    read_fn(rthread);
}
