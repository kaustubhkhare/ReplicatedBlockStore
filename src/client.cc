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

class GRPCClient{
private:
    std::unique_ptr<gRPCService::Stub> p_stub_;
    std::unique_ptr<gRPCService::Stub> b_stub_;
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
    GRPCClient(std::shared_ptr<Channel> p_channel, std::shared_ptr<Channel> b_channel) :
    p_stub_(gRPCService::NewStub(p_channel)), b_stub_(gRPCService::NewStub(b_channel)) {}

    std::string p_read(int address, int length) {
        LOG_DEBUG_MSG("Starting read");
        ds::ReadRequest readRequest;
        readRequest.set_address(address);
        readRequest.set_data_length(length);
        ds::ReadResponse readResponse;
        ClientContext context;

        LOG_DEBUG_MSG("Sending read to server");
        Status status = p_stub_->c_read(&context, readRequest, &readResponse);
        LOG_DEBUG_MSG("back from server");
//        if (!status.ok()) {
//            return -ENONET;
//        }
//        if (readResponse.ret() < 0) {
//            return readResponse.ret();
//        }
        return readResponse.data();
    }


    int write(int address, int length, const char* wr_buffer) {
        LOG_DEBUG_MSG("Starting client write");
        ds::WriteRequest writeRequest;
        writeRequest.set_address(address);
        writeRequest.set_data_length(length);
        writeRequest.set_data(wr_buffer);

        LOG_DEBUG_MSG("Sending write to server");
        Status status = p_stub_->c_write(&context, writeRequest, &writeResponse);
        LOG_DEBUG_MSG("Wrote to server ", writeResponse.bytes_written(), " bytes");

        if (!status.ok()){
            LOG_DEBUG_MSG("Error in writing ErrorCode: ", status.error_code(), " Error: ", status.error_message());
            return ENONET;
        }
//        if (writeResponse.ret() < 0) {
//            return writeResponse.ret();
//        }
        return writeResponse.bytes_written();
    }
};

int main(int argc, char *argv[]) {
    GRPCClient client(
        grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()),
        grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()));

    // test 1
    int data_size = 3;
    int address = 0;
//    auto buf = std::make_unique<std::string>(data_size, 'a');
    std::string buf(data_size, 'a');
    LOG_DEBUG_MSG("Writing ", buf);
    int bytes = client.write(address, data_size, buf.c_str());
    LOG_DEBUG_MSG(bytes, " bytes written");

    auto bufRead = client.p_read(address, data_size);
    LOG_DEBUG_MSG("Reading ", bufRead);

    // IMP TODO : check hash of full block not just the part written or read till data size
//    std::size_t hin = std::hash<std::string>{}(*buf);
//    std::size_t hout = std::hash<std::string>{}(*bufRead);
//    if (hin != hout) {
//        LOG_DEBUG_MSG("not equal");
//    } else {
//        LOG_DEBUG_MSG("equal");
//    }
    return 0;
}
