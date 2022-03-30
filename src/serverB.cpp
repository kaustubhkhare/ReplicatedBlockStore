#include "ds.grpc.pb.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <unistd.h>
#include <threads>

#include "constants.h"

using ds::gRPCService;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;

class gRPCServiceImpl final : public gRPCService::Service {
private:
    int fd;
    std::unordered_map<int, std::string> temp_data;
public:
    explicit gRPCServiceImpl(const std::string filename) {
        std::cout << __LINE__ << " calling constructor\n";
        fd = open(filename.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        if (fd < 0) {
            std::cout << "server_init: Cannot open file\n";
        }
        int ret = ftruncate(fd, constants::FILE_SIZE);
        if (ret < 0) {
            std::cout << "server_init: Cannot increase file size\n";
        }
        int size = lseek(fd, 0, SEEK_END);
        std::cout << "file size is " << size << "\n";
        lseek(fd, 0, SEEK_CUR);
    }

    // Returns the block indices for the offset and data_length. In this case return vector size is at most 2
    std::vector<int> get_blocks_involved(const int offset, const int data_length) {
        int first_block = offset / BLOCK_SIZE;
        int end_of_first_block = first_block + BLOCK_SIZE - 1,
                first_block_size_left = end_of_first_block - first_block * BLOCK_SIZE;
        std::vector blocks_involved;
        blocks_involved.push_back(first_block);
        if (data_length > first_block_size_left) {
            blocks_involved.push_back(first_block + 1);
        }
        return blocks_involved;
    }

    // only called by back up server,
    // virtual locks (not c++ mutex) -> lock for block 'x' is acquired if 'x' exists in temp_data with LOCK state
    void wait_before_read(const ds::ReadRequest *readRequest) {
        // todo: add a check if this is a backup. Primaries don't need to wait for reads
        std::vector<int> blocks = get_blocks_involved(readRequest->offset, BLOCK_SIZE);
        // change this to get signaled when the entry is removed from the map (write to that block is complete)

        boolean can_read_all = false;
        while(can_read_all) {
            can_read_all = true;
            for (const int &b: blocks) {
                if (temp_data[b] != null && temp_data[b]->state == BlockState::LOCKED) {
                    can_read_all = false;
                    break;
                }
            }
            if (!can_read_all) {
                std::this_thread.sleep_for(std::chrono::nanoseconds(1));
            }
        }
    }

    Status s_read(ServerContext *context, const ds::ReadRequest *readRequest,
                  ds::ReadResponse *readResponse) {
        wait_before_read();
        std::cout << __LINE__ << "\n";
        char* buf = (char*) calloc(constants::BLOCK_SIZE, sizeof(char));
        lseek(fd, readRequest->offset(), SEEK_SET);
        int bytes_read = read(fd, buf, constants::BLOCK_SIZE);
        std::cout << bytes_read << " bytes read\n";
        readResponse->set_data(buf);
        delete[] buf;
        return Status::OK;
    }

    Status s_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        std::cout << __LINE__ << "Starting backup server write\n";
        temp_data[writeRequest->offset()] = writeRequest->data();
        // send to backup
        // receive ack
        lseek(fd, writeRequest->offset(), SEEK_SET);
        int bytes = write(fd, &writeRequest->data(), constants::BLOCK_SIZE);
        writeResponse->set_bytes_written(bytes);
        // send commit msg to backup
        return Status::OK;
    }
};

//gRPCServiceImpl::~gRPCServiceImpl() {
//    std::cout << __LINE__ << "Calling destructor\n";
//    close(fd);
//}

int main(int argc, char *argv[]) {
    std::cout << __LINE__ << "Starting backup\n";
    std::cout.flush();
    std::string server_address("0.0.0.0:50052");
    gRPCServiceImpl service(argv[1]);
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.SetMaxSendMessageSize(INT_MAX);
    builder.SetMaxReceiveMessageSize(INT_MAX);
    builder.SetMaxMessageSize(INT_MAX);

    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}