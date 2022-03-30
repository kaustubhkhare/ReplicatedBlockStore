#include "ds.grpc.pb.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <unistd.h>

#include "constants.h"
#include "helper.h"

using ds::gRPCService;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

class gRPCServiceImpl final : public gRPCService::Service {
private:
    int fd;
    std::unordered_map<int, std::string> temp_data;
//    std::unique_ptr<gRPCService::Stub> stub_;
    enum class ServerState: int32_t { PRIMARY = 0, BACKUP };
    std::atomic<ServerState> current_server_state_;
    std::vector<std::mutex> per_block_locks;
public:
    auto get_server_state() const {
        return current_server_state_.load(std::memory_order_relaxed);
    }
    void transition(ServerState current_state) {
	    ASS(get_server_state() == current_state, "server state transition error");
	    ASS(current_server_state_.compare_exchange_strong(current_state, ServerState::PRIMARY),
			    "concurrent attempt to do state transition?");
    }
    void transition_to_primary() {
        transition(ServerState::BACKUP);
        LOG_INFO_MSG(" -> PRIMARY");
    }
    void transition_to_backup() {
        transition(ServerState::PRIMARY);
        LOG_INFO_MSG(" -> BACKUP");
    }

    explicit gRPCServiceImpl(const std::string filename,
                             bool primary = true) {
        per_block_locks.resize(TOTAL_BLOCKS);
        current_server_state_ = (primary) ? ServerState::PRIMARY : ServerState::BACKUP;
        LOG_DEBUG_MSG("constructor called");
        fd = open(filename.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        if (fd < 0) {
	        LOG_ERR_MSG("server_init: Cannot open file: " + filename);
        }
        int ret = ftruncate(fd, constants::FILE_SIZE);
        if (ret < 0) {
            LOG_ERR_MSG("server_init: Cannot increase file size\n");
        }
        int size = lseek(fd, 0, SEEK_END);
        LOG_DEBUG_MSG("file size is ", size);
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

    void get_write_locks(const ds::WriteRequest *writeRequest) {
        vector<int> blocks = get_blocks_involved(writeRequest->offset, writeRequest->data_length);
        if (blocks.size() == 1) {
            per_block_locks[blocks[0]].lock();
        } else {
            // max size can only be 2, same thing can be generalized to n, no need in this case.
            std::mutex first_block_lock = per_block_locks[blocks[0]],
                        second_block_lock = per_block_locks[blocks[1]];

            while(first_block_lock_acquired && second_block_lock_acquired) {
                // try acquiring the locks
                first_block_lock_acquired = !first_block_lock_acquired && first_block_lock.try_lock();
                second_block_lock_acquired = !first_block_lock_acquired && second_block_lock.try_lock();
                // if both obtained, will get out of loop, if not both obtained, release obtained locks
                if (!first_block_lock_acquired || !second_block_lock_acquired) {
                    if (first_block_lock_acquired) {
                        first_block_lock.release();
                    }
                    if (second_block_lock_acquired) {
                        second_block_lock.release();
                    }
                }
            }
        }
    }

    void release_write_locks(const ds::WriteRequest *writeRequest) {
        vector<int> blocks = get_blocks_involved(writeRequest->offset, writeRequest->data_length);
        for (const int &block: blocks) {
            per_block_locks[block].release();
        }
    }

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
        ds::ReadResponse *readResponse) {
//        LOG_DEBUG_MSG("Read called on ", current_server_state_);
//        char* buf = (char*) calloc(constants::BLOCK_SIZE, sizeof(char));
        auto buf = std::make_unique<std::string>(constants::BLOCK_SIZE, '\0');
        lseek(fd, readRequest->offset(), SEEK_SET);
        int bytes_read = read(fd, buf->data(), constants::BLOCK_SIZE);
        LOG_DEBUG_MSG("bytes read", bytes_read);
        readResponse->set_data(buf->data());
//        delete[] buf;
        return Status::OK;
    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
        ds::WriteResponse *writeResponse) {
        LOG_DEBUG_MSG("Starting server write");
        get_write_locks(writeRequest->offset);
//        temp_data[writeRequest->offset()] = writeRequest->data();
        // send to backup
//        Status status = stub_->s_write(&context, writeRequest, &ackResponse);
        // receive ack
        lseek(fd, writeRequest->offset(), SEEK_SET);
        std::cerr << writeRequest->data().length();
        int bytes = write(fd, &writeRequest->data(), writeRequest->data_length());
        std::cerr << bytes << "\n";
//        &writeRequest->data_length()
        writeResponse->set_bytes_written(bytes);
        // send commit msg to backup
        // release the locks only before returning to client
        release_write_locks(writeRequest);
        return Status::OK;
    }
};

//gRPCServiceImpl::~gRPCServiceImpl() {
//    LOG_MSG_DEBUG("Calling destructor\n");
//    close(fd);
//}

int main(int argc, char *argv[]) {
    std::cerr << "here\n";
    std::string server_address("0.0.0.0:50051");
    gRPCServiceImpl service(argv[1]);
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.SetMaxSendMessageSize(INT_MAX);
    builder.SetMaxReceiveMessageSize(INT_MAX);
    builder.SetMaxMessageSize(INT_MAX);
    std::cerr << "here\n";
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
//    LOG_INFO_MSG("Server listening on ", server_address);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}
