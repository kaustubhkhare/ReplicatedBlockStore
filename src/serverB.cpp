#include "ds.grpc.pb.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <unistd.h>
#include <future>

#include "helper.h"
#include "constants.h"

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
    enum class BlockState{ DISK, LOCKED};
    typedef struct {
        BlockState state;
        int length;
        std::string data;
    } Info;
    int fd;
    enum class BackupState {ALIVE, DEAD};
    std::atomic<BackupState> backup_state;
    enum class ServerState: int32_t { PRIMARY = 0, BACKUP };
    std::atomic<ServerState> current_server_state_;
    std::unique_ptr <gRPCService::Stub> stub_;
    std::unordered_map<int, Info*> temp_data;
    std::vector<std::future<Status>> pending_futures;

public:
    explicit gRPCServiceImpl(std::shared_ptr<Channel> channel, const std::string filename) :
            stub_(gRPCService::NewStub(channel)){
        LOG_DEBUG_MSG(" calling constructor");
        fd = open(filename.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        if (fd < 0) {
            LOG_DEBUG_MSG("server_init: Cannot open file");
        }
        int ret = ftruncate(fd, constants::FILE_SIZE);
        if (ret < 0) {
            LOG_DEBUG_MSG("server_init: Cannot increase file size");
        }
        int size = lseek(fd, 0, SEEK_END);
        LOG_DEBUG_MSG("file size is ", size);
        lseek(fd, 0, SEEK_CUR);
        backup_state = BackupState::ALIVE;
        current_server_state_ = ServerState::BACKUP;
    }

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
                  ds::ReadResponse *readResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("reading from primary");
            int buf_size = constants::BLOCK_SIZE;
            auto buf = std::make_unique<std::string>(buf_size, '\0');
            int bytes_read = pread(fd, buf->data(), buf_size, readRequest->address());
            LOG_DEBUG_MSG(bytes_read, " bytes read");
//            readResponse->set_data(buf.release());
        } else {
            // process at backup
            LOG_DEBUG_MSG("reading from backup");
        }
        return Status::OK;
    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("writing to primary");
//            for (int i = 0; i < pending_futures.size(); i++) {
//                if (pending_futures[i].valid()) {
//                        int address = pending_futures[i].get();
//                        temp_data.erase(address);
//                }
//            }
            LOG_DEBUG_MSG("Starting primary server write");
            BlockState state = BlockState::DISK;
            Info info = {state, writeRequest->data_length(), writeRequest->data()};
            temp_data[(int)writeRequest->address()] = &info;

            if (backup_state == BackupState::ALIVE) {
                ClientContext context;
                ds::AckResponse ackResponse;
                LOG_DEBUG_MSG("sending read to backup");
                Status status = stub_->s_write(&context, *writeRequest, &ackResponse);
                LOG_DEBUG_MSG("back from backup");
            }

            LOG_DEBUG_MSG("write from map to file");
            int bytes = pwrite(fd, &writeRequest->data(), writeRequest->data_length(), writeRequest->address());
            writeResponse->set_bytes_written(bytes);

            if (backup_state == BackupState::ALIVE) {
                LOG_DEBUG_MSG("commit to backup");
                ClientContext context;
                ds::CommitRequest commitRequest;
                commitRequest.set_address(writeRequest->address());
                ds::AckResponse ackResponse;
//                std::future<Status> f = std::async(std::launch::async,
//                    stub_->s_commit, &context, commitRequest, &ackResponse);
                Status status = stub_->s_commit(&context, commitRequest, &ackResponse);
//                pending_futures.push_back(std::move(f));
                LOG_DEBUG_MSG("committed to backup");
            }
            return Status::OK;
        }
        LOG_DEBUG_MSG("Starting backup server write");
    }
    Status s_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::AckResponse *ackResponse) {
        if (current_server_state_ == ServerState::BACKUP) {
            LOG_DEBUG_MSG("Starting backup server write");
            BlockState state = BlockState::LOCKED;
            Info info = {state, writeRequest->data_length(), writeRequest->data()};
            temp_data[(int) writeRequest->address()] = &info;
        } else {
            LOG_DEBUG_MSG("calling s_write at backup?");
        }
        return Status::OK;
    }

    Status s_commit(ServerContext *context, const ds::CommitRequest *commitRequest,
        ds::AckResponse *ackResponse) {
        LOG_DEBUG_MSG("calling commit on backup");
        BlockState diskState = BlockState::DISK;
        Info *info = temp_data[(int)commitRequest->address()];
        int bytes = pwrite(fd, info->data.c_str(), info->length, commitRequest->address());
        temp_data.erase((int)commitRequest->address());
        return Status::OK;
    }

    ~gRPCServiceImpl() {
        LOG_DEBUG_MSG("Calling destructor");
        close(fd);
    }
};

int main(int argc, char *argv[]) {
    LOG_DEBUG_MSG("Starting backup");
    std::string server_address("0.0.0.0:50053");
    gRPCServiceImpl service(grpc::CreateChannel("localhost:50052",
        grpc::InsecureChannelCredentials()), argv[1]);
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