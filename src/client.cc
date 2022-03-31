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
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "constants.h"
#include "helper.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using ds::gRPCService;
using ds::LBService;

class GRPCClient {
private:
    std::unique_ptr<LBService::Stub> lb_stub_;
    std::vector<std::unique_ptr<gRPCService::Stub>> server_stubs_;
    std::vector<std::string> servers;
    int primary_idx;
    int secondary_idx;
    long double lease_start;
    long double lease_duration;

    static std::string hash_str(const char* src) {
        auto digest = std::make_unique<unsigned char[]>(SHA256_DIGEST_LENGTH);
        SHA256(reinterpret_cast<const unsigned char*>(src), strlen(src),
               digest.get());
        std::stringstream ss;
        for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
            ss << std::hex << std::setw(2) << std::setfill('0')
               << static_cast<int>(digest[i]);
        }
        return ss.str();
    }
public:
    GRPCClient(std::shared_ptr<Channel> lb_channel) : lb_stub_(LBService::NewStub(lb_channel)) {}

    std::string read(int address, int length) {
        if (time_monotonic() > (lease_start + lease_duration))
            discover_servers(false);
        LOG_DEBUG_MSG("Starting read");
        ds::ReadResponse readResponse;
        ClientContext context;
        ds::ReadRequest readRequest;
        readRequest.set_data_length(length);
        readRequest.set_address(address);

        LOG_DEBUG_MSG("Sending read to server ", servers[secondary_idx]);
        Status status = server_stubs_[secondary_idx]->c_read(&context, readRequest, &readResponse);
        LOG_DEBUG_MSG("Read from server" + readResponse.data());

        if (!status.ok()) {
            LOG_DEBUG_MSG("Error in reading ErrorCode: ", status.error_code(), " Error: ", status.error_message());
            discover_servers(false);
            return "ERROR";
        }
        return readResponse.data();
    }

    int write(int address, int length, const char* wr_buffer) {
        if (time_monotonic() > (lease_start + lease_duration))
            discover_servers(false);
        LOG_DEBUG_MSG("Starting client write");
        ClientContext context;
        ds::WriteResponse writeResponse;
        ds::WriteRequest writeRequest;
        writeRequest.set_address(address);
        writeRequest.set_data_length(length);
        writeRequest.set_data(wr_buffer);

        LOG_DEBUG_MSG("Sending write to server");
        Status status = server_stubs_[primary_idx]->c_write(&context, writeRequest, &writeResponse);
        LOG_DEBUG_MSG("Wrote to server ", writeResponse.bytes_written(), " bytes");

        if (!status.ok()){
            LOG_DEBUG_MSG("Error in writing ErrorCode: ", status.error_code(), " Error: ", status.error_message());
            discover_servers(false);
            return ENONET;
        }
//        if (writeResponse.ret() < 0) {
//            return writeResponse.ret();
//        }
        return writeResponse.bytes_written();
    }

    void discover_servers(bool initialization) {

        LOG_DEBUG_MSG("Starting discovery with initialization ", initialization);
        ClientContext context;
        ds::ServerDiscoveryResponse response;
        ds::ServerDiscoveryRequest request;
        request.set_is_initial(initialization);

        Status status = lb_stub_->get_servers(&context, request, &response);

        if (!status.ok()) {
            LOG_DEBUG_MSG("Server discovery failed ErrorCode: ", status.error_code(), " Error: ", status.error_message());
        } else {
            if (initialization) {
                servers.clear();
                for (int i = 0; i < response.hosts_size(); i++) {
                    servers.push_back(response.hosts(i));
                    server_stubs_.push_back(gRPCService::NewStub(grpc::CreateChannel(servers[i],
                                                                                     grpc::InsecureChannelCredentials())));
                }
            }
            primary_idx = response.primary();
            secondary_idx = response.secondary();
            lease_start = response.lease_start();
            lease_duration = response.lease_duration();

            server_stubs_[primary_idx] = gRPCService::NewStub(grpc::CreateChannel(servers[primary_idx],
                                                                      grpc::InsecureChannelCredentials()));
            server_stubs_[secondary_idx] = gRPCService::NewStub(grpc::CreateChannel(servers[secondary_idx],
                                                                                  grpc::InsecureChannelCredentials()));

            LOG_DEBUG_MSG("Server discovery completed. Primary=", servers[primary_idx], " Secondary=", servers[secondary_idx]);
        }

    }
};

int main(int argc, char *argv[]) {
    if(argc < 5) {
        printf("Usage : ./client -ip <ip> -port <port>\n");
        return 0;
    }

    std::string ip{"0.0.0.0"}, port{"60051"};
    for(int i = 1; i < argc - 1; ++i) {
        if(!strcmp(argv[i], "-ip")) {
            ip = std::string{argv[i+1]};
        } else if(!strcmp(argv[i], "-port")) {
            port = std::string{argv[i+1]};
        }
    }
    std::string server_address(ip + ":" + port);
    LOG_DEBUG_MSG("Connecting to ", server_address);

    GRPCClient client(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
    client.discover_servers(true);

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
//    std::string bufRead = client.read(0);
////    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    std::cout << bufRead << "\n";
//    bufRead = client.read(5);
////    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    std::cout << bufRead << "\n";
    return 0;
}
