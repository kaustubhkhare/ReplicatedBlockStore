#include "test_helper.h"
#include "client.h"
#include <type_traits>
#include <iostream>
#include <cassert>
#include "client.h"
#include "../include/constants.h"

std::string get_string_with_length(int length, int iteration=1) {
    std::string a = std::to_string(iteration);
    a.append(length - a.size(), iteration);
}

int test4(int argc, char *argv[]) {
    std::string ip{"0.0.0.0"}, port{"7070"};
    for (int i = 1; i < argc - 1; ++i) {
        if(!strcmp(argv[i], "-ip")) {
            ip = std::string{argv[i+1]};
        } else if(!strcmp(argv[i], "-port")) {
            port = std::string{argv[i+1]};
        }
    }

    std::string server_address(ip + ":" + port);
    LOG_DEBUG_MSG("Connecting to ", server_address);

    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, constants::MAX_RECONN_TIMEOUT);

    GRPCClient client(grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), args));
    client.discover_servers(true);

    //read/write aligned reads/writes
    int iteration = 0;
    int length = 100, address_size = 100;
    std::vector<int> addresses;

    for (int i = 0; i < address_size; i++) {
        addresses.push_back(i*constants::BLOCK_SIZE); // unaligned
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
