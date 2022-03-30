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

#include "constants.h"
#include "helper.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using ds::gRPCService;

class HealthClient{
private:
    using uptr = std::unique_ptr<gRPCService::Stub>;
    std::vector<uptr> server_stubs_;
//    std::unique_ptr<gRPCService::Stub> p_stub_;
//    std::unique_ptr<gRPCService::Stub> b_stub_;
    std::vector<std::string> available_hosts;
    std::vector<std::string> dead_hosts;
    std::vector<std::string> targets;

    int round = 0;

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
    HealthClient(std::vector<std::shared_ptr<Channel>> channels, std::vector<std::string> t) {
        targets = t;
        for (auto channel: channels)
            server_stubs_.emplace_back(gRPCService::NewStub(channel));
    }

    void start_check() {

        while (true) {
            for (int i = 0; i < targets.size(); i++) {
                Status status;
                ClientContext context;
                ds::HBResponse response;
                ds::HBRequest request;
                request.set_is_primary(true);
                LOG_DEBUG_MSG("i=", i);
                status = server_stubs_[i]->hb_check(&context, request, &response);
                if (!status.ok()) {
                    LOG_DEBUG_MSG("Server ", targets[i], " did not respond.");
                } else {
                    LOG_DEBUG_MSG("Server ", targets[i], " is up.");
                }
            }
            sleep(1);
        }

    }

};

int main(int argc, char *argv[]) {

    std::vector<std::string> targets {"localhost:50052", "localhost:50053"};
    std::vector<std::shared_ptr<::grpc::Channel>> channels {
        grpc::CreateChannel(targets[0], grpc::InsecureChannelCredentials()),
        grpc::CreateChannel(targets[1], grpc::InsecureChannelCredentials())
    };

    HealthClient client(channels, targets);
    client.start_check();

    return 0;
}
