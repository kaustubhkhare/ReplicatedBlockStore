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
#include <sstream>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>

#include "constants.h"
#include "helper.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using ds::gRPCService;
using ds::LBService;

class LBServiceImpl final : public LBService::Service {
private:
    using uptr = std::unique_ptr<gRPCService::Stub>;
    std::vector<uptr> server_stubs_;
    std::set<int> available_hosts;
    std::set<int> dead_hosts;
    std::vector<std::string> targets;
    std::atomic<int> primary_idx;
    std::atomic<int> secondary_idx;

public:
    LBServiceImpl(std::vector<std::shared_ptr<Channel>> channels, std::vector<std::string> t) {
        targets = t;
        primary_idx.store(-1);
        secondary_idx.store(-1);
        for (auto channel: channels)
            server_stubs_.emplace_back(gRPCService::NewStub(channel));
    }

//    void assign_new_primary() {
//        if (available_hosts.size() == 0)
//            primary_idx.store(-1);
//        primary_idx.store(*available_hosts.begin());
//    }

    void assign_new_primary(int i) {
        primary_idx.store(i);
    }

//    void assign_new_secondary() {
//        secondary_idx.store((secondary_idx.load() + 1) % available_hosts.size());
//    }

    void assign_new_secondary(int i) {
        secondary_idx.store(i);
    }

    void start_check() {
        while (true) {
            for (int i = 0; i < targets.size(); i++) {
//                LOG_DEBUG_MSG("Testing ", targets[i]);
                Status status;
                ClientContext context;
                ds::HBResponse response;
                ds::HBRequest request;
//                request.set_is_primary(primary_idx.load() == i);
                status = server_stubs_[i]->hb_check(&context, request, &response);
//                LOG_DEBUG_MSG("Status ErrorCode: ", status.error_code(), " Error: ", status.error_message());
                if (!status.ok()) {
                    LOG_DEBUG_MSG("status not okay");
                    ClientContext context1;
                    if (available_hosts.count(i) > 0)
                        available_hosts.erase(i);
                    dead_hosts.insert(i);
                    if (primary_idx.load() == i) {
                        assign_new_primary(i);
                        request.set_is_primary(true);
                        request.set_sec_alive(false);
                        status = server_stubs_[primary_idx.load()]->hb_tell(&context1, request, &response);
                        secondary_idx.store(-1);
                        if (!status.ok()) {
                            LOG_ERR_MSG("Set primary ", primary_idx.load(), " also not available. Setting primary -1");
                            primary_idx.store(-1);
                        } else {
                            LOG_DEBUG_MSG("Primary at", targets[i], "went down, set", targets[(i + 1) % 2],
                                          "as primary");
                        }
                    } else if (secondary_idx.load() == i){
                        request.set_is_primary(true);
                        request.set_sec_alive(false);
                        status = server_stubs_[primary_idx.load()]->hb_tell(&context1, request, &response);
                        secondary_idx.store(-1);
                        if (!status.ok()) {
                            LOG_ERR_MSG("Set primary ", primary_idx.load(), " also not available. Setting primary -1. Secondary also dead.");
                            primary_idx.store(-1);
                        } else
                            LOG_DEBUG_MSG("Backup at", targets[i], "went down, setting backup as dead");
                    } else {
                        //
                    }
                    LOG_DEBUG_MSG("Server ", targets[i], " did not respond.");
                } else if (status.ok()) {
                    LOG_DEBUG_MSG("status okay");
                    ClientContext context2;
//                    LOG_DEBUG_MSG("Server ", targets[i], " did respond.");
                    available_hosts.insert(i);
                    if (dead_hosts.count(i) > 0) {
                        dead_hosts.erase(i);
//                        secondary_idx.store(i);
                    }
                    if (primary_idx.load() == -1) {
                        assign_new_primary(i);
                        request.set_is_primary(true);
                        request.set_sec_alive(false);
                        status = server_stubs_[i]->hb_tell(&context2, request, &response);
                        LOG_DEBUG_MSG("Primary not set, setting ", targets[i], "as primary");
                    }
                    else if (secondary_idx.load() == -1 && primary_idx.load() != i){
                        request.set_is_primary(false);
                        request.set_sec_alive(false);
                        status = server_stubs_[i]->hb_tell(&context2, request, &response);
                        secondary_idx.store(i);

                        LOG_DEBUG_MSG("Secondary not set, setting", targets[i], "as secondary");
                    } else {
                        //do nothing
                    }
                } else {
                    LOG_DEBUG_MSG("error");
                    //
                }
            }
            sleep(1);
        }
    }

    Status get_servers(ServerContext *context, const ds::ServerDiscoveryRequest *request,
                       ds::ServerDiscoveryResponse *response) {
        if (request->is_initial()) {
            LOG_DEBUG_MSG("Initialization request");
            for (auto target: targets)
                response->add_hosts(target);
        }

//        assign_new_secondary();
        response->set_primary(primary_idx.load());
        response->set_secondary(secondary_idx.load());
        response->set_lease_start(time_monotonic());
        response->set_lease_duration(2e9);

        return Status::OK;
    }

};

std::vector<std::string> get_separated(std::string str, char delimiter) {
    std::vector<std::string> v;
    std::stringstream ss(str);

    while (ss.good()) {
        std::string substr;
        getline(ss, substr, delimiter);
        v.push_back(substr);
    }

    return v;
}

int main(int argc, char *argv[]) {
    if (argc < 7) {
        printf("Usage : ./health_client -ip <ip> -port <port> -hosts <comma sep host:port>\n");
        return 0;
    }

    std::string ip{"0.0.0.0"}, port{"60051"};
    std::vector<std::string> v;
    for (int i = 1; i < argc - 1; ++i) {
        if (!strcmp(argv[i], "-ip")) {
            ip = std::string{argv[i+1]};
        } else if (!strcmp(argv[i], "-port")) {
            port = std::string{argv[i+1]};
        } else if (!strcmp(argv[i], "-hosts")) {
            v = get_separated(std::string(argv[i + 1]), ',');
        }
    }
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, constants::MAX_RECONN_TIMEOUT);

    std::string server_address(ip + ":" + port);
    std::vector<std::string> targets {v[0],
                                      v[1]};
    std::vector<std::shared_ptr<::grpc::Channel>> channels {
        grpc::CreateCustomChannel(targets[0], grpc::InsecureChannelCredentials(), args),
        grpc::CreateCustomChannel(targets[1], grpc::InsecureChannelCredentials(), args)
    };

    LBServiceImpl service(channels, targets);

//    std::thread t1(&LBServiceImpl::start_check, &service);
    auto t1 = std::thread([&]() { service.start_check(); });
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.SetMaxSendMessageSize(INT_MAX);
    builder.SetMaxReceiveMessageSize(INT_MAX);
    builder.SetMaxMessageSize(INT_MAX);
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();

    t1.join();

    return 0;
}