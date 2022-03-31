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
    LBServiceImpl(std::vector<std::shared_ptr<Channel>> channels, std::vector<std::string> t) {
        targets = t;
        primary_idx.store(-1);
        for (auto channel: channels)
            server_stubs_.emplace_back(gRPCService::NewStub(channel));
    }

    void assign_new_primary() {
        if (available_hosts.size() == 0)
            primary_idx.store(-1);
        primary_idx.store(*available_hosts.begin());
    }

    void assign_new_secondary() {
        secondary_idx.store((secondary_idx.load() + 1) % available_hosts.size());
    }

    void start_check() {
        while (true) {
            for (int i = 0; i < targets.size(); i++) {
                Status status;
                ClientContext context;
                ds::HBResponse response;
                ds::HBRequest request;
                request.set_is_primary(primary_idx.load() == i);
                status = server_stubs_[i]->hb_check(&context, request, &response);
                if (!status.ok()) {
                    if (available_hosts.count(i) > 0)
                        available_hosts.erase(i);
                    dead_hosts.insert(i);
                    if (primary_idx.load() == i)
                        assign_new_primary();
                    LOG_DEBUG_MSG("Server ", targets[i], " did not respond.");
                } else {
                    available_hosts.insert(i);
                    if (dead_hosts.count(i) > 0)
                        dead_hosts.erase(i);
                    if (primary_idx.load() == -1)
                        assign_new_primary();
//                    LOG_DEBUG_MSG("Server ", targets[i], " is up.");
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

        assign_new_secondary();
        response->set_primary(primary_idx.load());
        response->set_secondary(secondary_idx.load());

        return Status::OK;
    }

};

int main(int argc, char *argv[]) {

    if(argc < 5) {
        printf("Usage : ./health_client -ip <ip> -port <port>\n");
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
    std::vector<std::string> targets {"localhost:60052", "localhost:60053"};
    std::vector<std::shared_ptr<::grpc::Channel>> channels {
        grpc::CreateChannel(targets[0], grpc::InsecureChannelCredentials()),
        grpc::CreateChannel(targets[1], grpc::InsecureChannelCredentials())
    };

    LBServiceImpl service(channels, targets);

    std::thread t1(&LBServiceImpl::start_check, &service);

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
