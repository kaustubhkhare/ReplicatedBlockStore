#include <thread>
#include <iostream>
#include <vector>
#include <future>
template <class C>
void run_test(C& client, const std::vector<int>& read_addr, const std::vector<int>& write_addr, 
            int rthread = 1, int wthread = 1, bool rw_seq = true)
{
    auto read_fn = [&] (const int n_threads) {
        std::vector<std::thread> threads;
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
