#include "client.h"
#include <type_traits>
#include <iostream>
#include <cassert>
#include "client.h"


#include "../include/constants.h"
#include <cstdlib>

std::string get_string_with_length_3(int length, int iteration=1) {
    std::string a = std::to_string(iteration);
    a.append(length - a.size(), '0' + (rand() % 10));
    return a;
}

int test3(int argc, char *argv[]) {
    if (argc < 5) {
        printf("Usage : ./client -lb <lbIp:port> -compare <1 or 0>\n");
        return 0;
    }

    std::string server_address{"localhost:60052"}, cmp{"0"};
    for (int i = 1; i < argc - 1; ++i) {
        if(!strcmp(argv[i], "-lb")) {
            server_address = std::string{argv[i+1]};
        } else if(!strcmp(argv[i], "-compare")) {
            cmp = std::string{argv[i+1]};
        }
    }
    LOG_DEBUG_MSG("Connecting to ", server_address);

    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, constants::MAX_RECONN_TIMEOUT);

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
//        sleep(2);
        std::cout << "Iteration #" << i++ << '\n';
        int offset = addresses[iteration % address_size];
        std::string v = get_string_with_length_3(length, iteration++);
        std::cout << "Write @ " << offset << "value:" << v << '\n';
        int status = client->write(offset, v.size(), v.c_str());
        if (status != ENONET) {
            std::cout << "Status not ENONET\n";
            std::cout << "Read @ " << offset << '\n';
            std::string read_str = client->read(offset, length);
        }
    }

    return 0;
}

