#include "zipf.h"
#include "test_helper.h"
#include <iostream>

struct ClientInterface {
    std::string p_read(int addr) {
        std::cout << "[R] " << addr << '\n';
        return "AAAA";
    }
    void p_write(int addr, int sz, const char* buf) {
        std::cout << "[W] " << addr << "-> " << std::string(buf, sz) << '\n';
    }
};

int main() {
    const int ADDR_LIMIT = 100;
    zipf_distribution<int> zipf(ADDR_LIMIT);
    uniform_distribution<int> unif(0, ADDR_LIMIT);
    const int N_READ = 200;
    const int N_WRITE = 100;
    auto read_v = zipf.get(N_READ);
    auto write_v = unif.get(N_WRITE);
    ClientInterface client;
    run_test(client, read_v, write_v, 4, 2, false);
}
