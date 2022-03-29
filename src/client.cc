#include "ds.grpc.pb.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <signal.h>
#include <chrono>
#include <ctime>
#include <vector>
#include <unordered_map>
#include <numeric>
#include <fstream>
#include <memory>

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
public:
    GRPCClient(std::shared_ptr<Channel> channel1, std::shared_ptr<Channel> channel2) :
    p_stub_(gRPCService::NewStub(channel1)), b_stub_(gRPCService::NewStub(channel2)) {
        //
    }

    std::string read(int offset) {
        LOG_DEBUG_MSG("Starting read");
        ds::ReadRequest readRequest;
        readRequest.set_offset(offset);
        ds::ReadResponse readResponse;
        ClientContext context;
        LOG_DEBUG_MSG("sending read to server");
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

    int write(int offset, int length, const char* wr_buffer) {
        LOG_DEBUG_MSG("Starting client write");
        ds::WriteRequest writeRequest;
        writeRequest.set_offset(offset);
        writeRequest.set_data_length(length);
        writeRequest.set_data(wr_buffer);
        ds::WriteResponse writeResponse;
        ClientContext context;
        LOG_DEBUG_MSG("Sending write to server");
        Status status = p_stub_->c_write(&context, writeRequest, &writeResponse);
        LOG_DEBUG_MSG("Back from server");
        LOG_DEBUG_MSG(writeResponse.bytes_written(), " bytes written");
        if (!status.ok()) {
            return -ENONET;
        }
//        if (writeResponse.ret() < 0) {
//            return writeResponse.ret();
//        }
        return writeResponse.bytes_written();
    }
};

int main(int argc, char *argv[]) {
    GRPCClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()),
                      grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
//    std::unique_ptr<std::string> buf = std::make_unique<std::string>(constants::BLOCK_SIZE, 'a');
    std::string buf(2, 'a');
    LOG_DEBUG_MSG(buf.size());
    int bytes = client.write(0, buf.length(), buf.c_str());
    LOG_DEBUG_MSG(bytes);

    std::string bufRead = client.read(0);
    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    bufRead.resize(2);
    std::cout << buf << " "<< bufRead;
//    if (buf.compare(bufRead) != 0) {
//        LOG_DEBUG_MSG("not equal");
//    } else {
//        LOG_DEBUG_MSG("equal");
//    }
    return 0;
}
