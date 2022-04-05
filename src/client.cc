#include "ds.grpc.pb.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <openssl/sha.h>
#include <iomanip>
#include <iostream>
#include <signal.h>
#include <chrono>
#include <ctime>
#include <vector>
#include <unordered_map>
#include <numeric>
#include <fstream>
#include <memory>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "constants.h"
#include "helper.h"
#include "client.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using ds::gRPCService;
using ds::LBService;


int main(int argc, char *argv[]) {
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

    while (true) {
        std::string command;
        int offset;
        int length;
        std::cout << "Enter command (r/w/x)";
        std::cin >> command;
        if (command == "r") {
            std::cout << "Enter offset:";
            std::cin >> offset;
            std::cout << "Enter length:";
            std::cin >> length;
            std::string read_str = client.read(offset, length);
            std::cout << read_str << "\n";
        } else if (command == "w") {
            std::string v;
            std::cout << "Enter offset:";
            std::cin >> offset;
            std::cout << "Enter length:";
            std::cin >> length;
            std::cout << "Enter string:";
            std::cin >> v;
            while (v.length() != length) {
                std::cout << "Length of string should be " << length << "\n";
                std::cout << "Enter string:";
                std::cin >> v;
            }
            client.write(offset, length, v.c_str());
        } else {
            break;
        }
    }
//    std::string buf(10, 'b');
//    int v = client.write(1000, buf.length(), buf.c_str());
//    std::string bufRead = client.read(1000, 10);
////    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    std::cout << "Writing" << buf << "\n";
////    bufRead = client.read();
////    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    std::cout << "Reading" << bufRead << "\n";
    return 0;
}
