#include "../include/constants.h"
#include <cstdlib>     /* srand, rand */

std::string get_string_with_length(int length, int iteration=1) {
    std::string a = std::to_string(iteration);
    a.append(length - a.size(), iteration);
}
int main(int argc, char *argv[]) {


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
        addresses.push_back(i + rand() % constants::BLOCK_SIZE);
    }

    while (1) {
        int offset = addresses[iteration % address_size];
        std::string v = get_string_with_length(length, iteration++);

        client.write(offset, v.size(), v.c_str());
        std::string read_str = client.read(offset, length);
        ASS(!v.strcmp(read_str), std::to_string("WRITE AND READ NOT SAME:") +
                                                v +
                                                std::to_string(",") +
                                                read_str);
    }

    return 0;
}