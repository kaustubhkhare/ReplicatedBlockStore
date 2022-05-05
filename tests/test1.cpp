#include "zipf.h"
#include "test_helper.h"
#include <type_traits>
#include <iostream>
#include <cassert>
#include "client.h"

enum class Distribution { ZIPF, UNIF };

struct TestTemplate {
    std::string name;
    struct AddrTemplate {
        Distribution d;
        size_t n;
        float aligned_ratio;  
    };
    AddrTemplate reads, writes;
    int rthreads, wthreads;
};

struct ClientInterface {
    GRPCClient *client;

    std::string read(int addr, int length) {
//        std::cerr << "Read: " << addr << "\n";
        return client->read(addr, length);
    }
    void write(int addr, int sz, const char* buf) {
//        std::cerr << "Write: " << addr << "\n";
        client->write(addr, sz, buf);
    }

    void reset() {}
};

const std::vector<double> RW_RATIOS = {1};
const std::vector<std::pair<int, int> > RW_THREADS
            = {
//               {1, 0}, {2, 0}, {4, 0}, {8, 0}, // all reads
//               {0, 1}, {0, 2}, {0, 4}, {0, 8}, // all writes
//               {1, 1}, {4, 1}, {1, 4}          // mix
                {4, 1}
            };

const std::vector<int> NUM_OPS = {(int)1e4};
const std::vector<double> ALIGNED_OPS_RATIO = {1};


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

int test1(int argc, char** argv) {
    ClientInterface client;
    client.client = GRPCClient::get_client(argc, argv);
    for (auto aligned_ratio: ALIGNED_OPS_RATIO) {
        for (auto ops: NUM_OPS) {
            const int ADDR_LIMIT = 2e3;
            const auto distr = Distribution::ZIPF;
            zipf_distribution<int> zipf(ADDR_LIMIT);
//            uniform_distribution<int> unif(0, ADDR_LIMIT);
            for (auto rw_ratio: RW_RATIOS) {
                const double rw_sum = (rw_ratio + 1);
                const auto n_read = ops * (rw_ratio / rw_sum);
                const auto n_write = ops - n_read;
                const auto read_v = get(zipf, n_read, aligned_ratio);
                const auto write_v = get(zipf, n_write, aligned_ratio);
		std::cerr << read_v.size() << " " << write_v.size() << "\n";
                for (auto [rthread, wthread]: RW_THREADS) {
                    const auto test_name = make_comma_sep("test", aligned_ratio, ops, rw_ratio, rthread, wthread);
                    std::cerr << "running: " << test_name << " [" << read_v.size() << ", " << write_v.size() << "]\n";
                    run_test(test_name, client, read_v, write_v, rthread, wthread, false);
                    client.reset();
                }
            }
        }
    }

    return 0;
}
