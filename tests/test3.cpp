#include "test_helper.h"
#include "client.h"
#include <type_traits>
#include <iostream>
#include <cassert>
#include "client.h"


#include "../include/constants.h"
#include <cstdlib>

std::string get_string_with_length(int length, int iteration=1) {
    std::string a = std::to_string(iteration);
    a.append(length - a.size(), iteration);
}

int test3(int argc, char *argv[]) {
    auto client = GRPCClient::get_client(argc, argv);
    //read/write aligned reads/writes
    int iteration = 0;
    int length = 100, address_size = 100;
    std::vector<int> addresses;

    for (int i = 0; i < address_size; i++) {
        addresses.push_back(i + rand() % constants::BLOCK_SIZE);
    }
    int i = 0;
    while (1) {
        std::cout << "Iteration #" << i++ << '\n';
        int offset = addresses[iteration % address_size];
        std::string v = get_string_with_length(length, iteration++);
        std::cout << "Write @ " << offset << '\n';
        client.write(offset, v.size(), v.c_str());
        std::cout << "Read @ " << offset << '\n';
        std::string read_str = client.read(offset, length);
        ASS(!v.compare(read_str), std::string("WRITE AND READ NOT SAME:") +
                                 v +
                                 std::string(",") +
                                 std::to_string(read_str));
    }

    return 0;
}

