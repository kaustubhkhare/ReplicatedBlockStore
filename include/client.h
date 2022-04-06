#pragma once
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
#include "ds.grpc.pb.h"
#include <stdio.h>
#include <stdlib.h>
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
    std::string compare;

    inline static std::string hash_str(const char* src) {
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
    inline static GRPCClient* get_client(int argc, char** argv) {
        if (argc < 5) {
            printf("Usage : ./client -lb <lbIp:port> -compare <1 or 0>\n");
            return NULL;
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

        auto client = new GRPCClient(grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), args), cmp);
        client->discover_servers(true);

        return client;
    }

    GRPCClient(std::shared_ptr<Channel> lb_channel, std::string cmp) : lb_stub_(LBService::NewStub(lb_channel)) {
        compare = cmp;
    }

   inline std::string read(int address, int length, int retry = 10) {
        if (time_monotonic() > (lease_start + lease_duration))
            discover_servers(false);
//        LOG_DEBUG_MSG("Starting read");
        ds::ReadResponse readResponse_p;
        ds::ReadResponse readResponse_b;
        ClientContext context;
        ds::ReadRequest readRequest;
        readRequest.set_data_length(length);
        readRequest.set_address(address);
        Status status;

        int server = rand() % 2;
        if (secondary_idx != -1) {
            if (compare == "1") {
                ClientContext context;
//                LOG_DEBUG_MSG("Sending read to secondary ", servers[secondary_idx]);
                status = server_stubs_[secondary_idx]->c_read(&context, readRequest, &readResponse_b);
//                LOG_DEBUG_MSG("Read from server" + readResponse_b.data());

                ClientContext context2;
//                LOG_DEBUG_MSG("Sending read to primary ", servers[primary_idx]);
                status = server_stubs_[primary_idx]->c_read(&context2, readRequest, &readResponse_p);
//                LOG_DEBUG_MSG("Read from server" + readResponse_p.data());

                if (hash_str(readResponse_p.data().c_str()) != hash_str(readResponse_b.data().c_str())) {
                    LOG_DEBUG_MSG("primary backup data not matching");
                } else {
//                    LOG_DEBUG_MSG("data committed to backup");
                }
            } else {
                if (server == 1) {
                    ClientContext context;
//                    LOG_DEBUG_MSG("Sending read to secondary ", servers[secondary_idx]);
                    status = server_stubs_[secondary_idx]->c_read(&context, readRequest, &readResponse_b);
//                    LOG_DEBUG_MSG("Read from server" + readResponse_b.data());
                } else {
                    ClientContext context2;

//                    LOG_DEBUG_MSG("Sending read to primary ", servers[primary_idx]);
                    status = server_stubs_[primary_idx]->c_read(&context2, readRequest, &readResponse_p);
//                    LOG_DEBUG_MSG("Read from server" + readResponse_p.data());
                }
            }
        } else {
//            LOG_DEBUG_MSG("Sending read to primary ", servers[primary_idx]);
            if (primary_idx == -1) {
                LOG_ERR_MSG("No server found for write. Retrying ", (10 - retry + 1));
                if (retry > 0)
                    return read(address, length, retry - 1);
                return "ERROR";
            }
            status = server_stubs_[primary_idx]->c_read(&context, readRequest, &readResponse_p);
//            LOG_DEBUG_MSG("Read from server" + readResponse_p.data());
        }

        if (!status.ok()) {
            LOG_ERR_MSG("Error in reading ErrorCode: ", status.error_code(),
                          " Error: ", status.error_message());
            discover_servers(false);
            if (retry > 0) {
                LOG_DEBUG_MSG("Retrying read", (10 - retry + 1));
                return read(address, length, retry - 1);
            }
            return "ERROR";
        }
        return readResponse_p.data();
    }

   inline int write(int address, int length, const char* wr_buffer, int retry = 10) {
        if (time_monotonic() > (lease_start + lease_duration))
            discover_servers(false);
        LOG_DEBUG_MSG("Starting client write");
        ClientContext context;
        ds::WriteResponse writeResponse;
        ds::WriteRequest writeRequest;
        writeRequest.set_address(address);
        writeRequest.set_data_length(length);
        writeRequest.set_data(wr_buffer);

//        LOG_DEBUG_MSG("Sending write to server");
//        LOG_DEBUG_MSG("primary is ", servers.at(primary_idx));
        if (primary_idx == -1) {
            LOG_ERR_MSG("No primary server found for write. Retrying ", (10 - retry + 1));
            if (retry > 0)
                return write(address, length, wr_buffer, retry - 1);
            return ENONET;
        }

        Status status = server_stubs_[primary_idx]->c_write(&context, writeRequest, &writeResponse);

//        LOG_DEBUG_MSG("Wrote to server ", writeResponse.bytes_written(), " bytes");

        if (!status.ok()){
            LOG_ERR_MSG("Error in writing ErrorCode: ", status.error_code(), " Error: ", status.error_message());
            discover_servers(false);
            if (retry > 0) {
                LOG_DEBUG_MSG("Retrying write", (10 - retry + 1));
                return write(address, length, wr_buffer, retry - 1);
            }
            return ENONET;
        }
        return writeResponse.bytes_written();
    }

   inline void discover_servers(bool initialization) {
        LOG_DEBUG_MSG("Starting discovery with initialization ", initialization);
        ClientContext context;
        ds::ServerDiscoveryResponse response;
        ds::ServerDiscoveryRequest request;
        request.set_is_initial(initialization);

        Status status = lb_stub_->get_servers(&context, request, &response);

        if (!status.ok()) {
            LOG_ERR_MSG("Server discovery failed ErrorCode: ", status.error_code(), " Error: ", status.error_message());
        } else {
            if (initialization) {
                servers.clear();
                grpc::ChannelArguments args;
                args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, constants::MAX_RECONN_TIMEOUT);

                for (int i = 0; i < response.hosts_size(); i++) {
                    servers.push_back(response.hosts(i));
                    server_stubs_.push_back(gRPCService::NewStub(grpc::CreateCustomChannel(servers[i],
                        grpc::InsecureChannelCredentials(), args)));
                }
            }
            primary_idx = response.primary();
            secondary_idx = response.secondary();
            lease_start = response.lease_start();
            lease_duration = response.lease_duration();
            LOG_DEBUG_MSG("primary_idx:", primary_idx, " secondary_idx:", secondary_idx);

//            server_stubs_[primary_idx] = gRPCService::NewStub(
//                    grpc::CreateChannel(servers[primary_idx], grpc::InsecureChannelCredentials()));
//            server_stubs_[secondary_idx] = gRPCService::NewStub(
//                    grpc::CreateChannel(servers[secondary_idx], grpc::InsecureChannelCredentials()));

            LOG_DEBUG_MSG("Server discovery completed. Primary=", servers[primary_idx], " Secondary=", servers[secondary_idx]);
        }

    }
};
