#include "zipf.h"
#include "test_helper.h"
#include "client.h"
#include <type_traits>
#include <iostream>
#include <cassert>
#include "client.h"


int test2(int argc, char** argv) {
    auto client = GRPCClient::get_client(argc, argv);
    zipf_distribution<int> zipf(FOUR_K * 3);
    const int nwrite = 1e3;
    auto write_v = get(zipf, nwrite, 0);
    run_test("3_thread_write_same_addr", client, {}, write_v, 0, 3, false);
    return 0;
}
