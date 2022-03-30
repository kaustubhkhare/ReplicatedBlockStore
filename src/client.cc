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

    std::string read(int address) {
        LOG_DEBUG_MSG("Starting read");
        ds::ReadResponse readResponse;
        ClientContext context;
        ds::ReadRequest readRequest;
        readRequest.set_address(address);

        LOG_DEBUG_MSG("Sending read to server");
        Status status = p_stub_->c_read(&context, readRequest, &readResponse);
        LOG_DEBUG_MSG("Read from server" + readResponse.data());

        if (!status.ok()) {
            LOG_DEBUG_MSG("Error in reading ErrorCode: ", status.error_code(), " Error: ", status.error_message());
            return "ERROR";
        }
        return readResponse.data();
    }

    int write(int address, int length, const char* wr_buffer) {
        LOG_DEBUG_MSG("Starting client write");
        ClientContext context;
        ds::WriteResponse writeResponse;
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
    GRPCClient client(grpc::CreateChannel("localhost:50052", grpc::InsecureChannelCredentials()),
                      grpc::CreateChannel("localhost:50053", grpc::InsecureChannelCredentials()));
    std::string buf(10, 'b');
    LOG_DEBUG_MSG(buf.size());
    int bytes = client.write(4000, buf.length(), buf.c_str());
    LOG_DEBUG_MSG(bytes);

    std::string bufRead = client.read(0);
//    std::cout << bufRead[0] << bufRead[1]<<"\n";
    std::cout << buf << " "<< bufRead;
    if (buf.compare(bufRead) != 0) {
        LOG_DEBUG_MSG("not equal");
    } else {
        LOG_DEBUG_MSG("equal");
    }
    return 0;
}
