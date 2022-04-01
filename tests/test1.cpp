#include "zipf.h"
#include "test_helper.h"
#include <type_traits>
#include <iostream>
#include <cassert>

struct ClientInterface {
    std::string p_read(int addr) {
        std::cout << "[R] " << addr << '\n';
        return "AAAA";
    }
    void p_write(int addr, int sz, const char* buf) {
        std::cout << "[W] " << addr << "-> " << std::string(buf, sz) << '\n';
    }

    void reset() {}
};

const std::vector<double> RW_RATIOS = {4, 2, 1, 0.5, 0.25};
const std::vector<std::pair<int, int> > RW_THREADS
            = {{1, 0}, {2, 0}, {4, 0}, {8, 0}, // all reads
               {0, 1}, {0, 2}, {0, 4}, {0, 8}, // all writes
               {1, 1}, {4, 1}, {1, 4}          // mix
            };
const std::vector<int> NUM_OPS = {(int)5e3, (int)1e4, (int)3e4};
const std::vector<double> ALIGNED_OPS_RATIO = {1, 0.5};

enum class Distribution { ZIPF, UNIF };

template <class T, class... Ts>
std::string make_comma_sep(T&& t, Ts&&... ts) {
    using Ty = std::decay_t<std::remove_reference_t<std::remove_cv_t<T>>>;
    std::string s;
    if constexpr(std::is_arithmetic_v<Ty>) {
        s = std::to_string(t);
    } else if constexpr(std::is_same_v<Ty, const char*>) {
        s = std::string(t);
    } else {
        assert(0);
    }
    s += ", ";
    if constexpr (sizeof...(Ts) > 0)
         s += make_comma_sep(ts...);
    return s;
}

int main() {
//    const int ADDR_LIMIT = ;
//    auto read_v = zipf.get(N_READ);
//    auto write_v = unif.get(N_WRITE);
    ClientInterface client;
    for (auto aligned_ratio: ALIGNED_OPS_RATIO) {
        for (auto ops: NUM_OPS) {
            const int ADDR_LIMIT = 2 * ops;
            const auto distr = Distribution::ZIPF;
            zipf_distribution<int> zipf(ADDR_LIMIT);
//            uniform_distribution<int> unif(0, ADDR_LIMIT);
            for (auto rw_ratio: RW_RATIOS) {
                const double rw_sum = (rw_ratio + 1);
                const auto n_read = ops * (rw_ratio / rw_sum);
                const auto n_write = ops - n_read;
                const auto read_v = get(zipf, n_read, aligned_ratio);
                const auto write_v = get(zipf, n_read, aligned_ratio);
                for (auto [rthread, wthread]: RW_THREADS) {
                    const std::string test_name = "test, " + std::to_string(aligned_ratio) + ", "
                            + std::to_string(ops) + ", " + std::to_string(rw_ratio) + ", " + std::to_string(rthread)
                            + ", " + std::to_string(wthread);
                    run_test(test_name, client, read_v, write_v, rthread, wthread, false);
                    client.reset();
                }
            }
        }
    }

//    run_test(client, read_v, write_v, 4, 2, false);
    return 0;
}
