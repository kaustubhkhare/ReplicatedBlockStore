//
// Created by Kaustubh Khare on 4/6/22.
//
#include "zipf.h"
#include "test_helper.h"
#include <type_traits>
#include <iostream>
#include <cassert>
#include "client.h"
struct ClientInterface {
    GRPCClient *client;

    std::string read(int addr, int length) {
        return client->read(addr, length);
    }
    void write(int addr, int sz, const char* buf) {
        client->write(addr, sz, buf);
    }

    void reset() {}
};

int test5(int argc, char** argv) {
    ClientInterface client;
    int k = std::stoi(std::string(argv[1]));
    argc--; argv++;
    client.client = GRPCClient::get_client(argc, argv);

    char buf[4096];
    memset(buf, 'X', sizeof(buf));

    while (k--) {
        client.write(100, 4096, buf);
        std::cerr << __LINE__ << " done\n";
        client.write(1e5, 4096, buf);
        std::cerr << __LINE__ << " done\n";
//        client.write(1000, 4096, buf);
//        client.write(9000, 4096, buf);
//        client.write(9100, 4096, buf);
//        client.write(9200, 4096, buf);
    }
    return 0;
}