#include "ds.grpc.pb.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>
#include <iostream>
#include <fstream>

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
    enum class BlockState{ DISK, LOCKED};
    typedef struct {
        BlockState state;
        int length;
        std::string data;
    } Info;
    std::string filename;
    enum class BackupState {ALIVE, DEAD};
    std::atomic<BackupState> backup_state;
    enum class ServerState: int32_t { PRIMARY = 0, BACKUP };
    std::atomic<ServerState> current_server_state_;
    std::unique_ptr <gRPCService::Stub> stub_;
    std::unordered_map<int, Info*> temp_data;
//    std::vector<std::future<Status>> pending_futures;

public:
    void create_file(const std::string filename) {
        std::fstream stream;
        stream.open(filename, std::fstream::in | std::fstream::out | std::fstream::app);

        if (!stream.is_open()) {
            std::cout << "File doesn't exist. Creating file.";
            stream.open(filename,  std::fstream::in | std::fstream::out | std::fstream::trunc);
            stream.close();
        }
    }

    int write(const char* buf, int offset, int size) {
        std::fstream outfile;
        LOG_DEBUG_MSG("Opening file ", filename, " for writing");
        outfile.open(filename);
        outfile.seekp(offset);
        outfile.write(buf, size);
        outfile.close();

        return size;
    }

    int read(char* buf, int offset, int size) {
        std::ifstream infile;
        LOG_DEBUG_MSG("Opening file ", filename, " for reading");
        infile.open(filename, std::ios::binary);
        infile.seekg(offset);
        infile.read(buf, size);
        infile.close();

        return size;
    }

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

    explicit gRPCServiceImpl(std::shared_ptr<Channel> channel,
                             const std::string filename, bool primary = true) :
            stub_(gRPCService::NewStub(channel)) {
        current_server_state_ = (primary)? ServerState::PRIMARY : ServerState::BACKUP;
        this->filename = filename;
        create_file(filename);
        LOG_DEBUG_MSG("Filename ", this->filename, " f:", filename);
        backup_state = BackupState::ALIVE;
        current_server_state_ = ServerState::PRIMARY;
    }

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
        ds::ReadResponse *readResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("reading from primary");
            char *buf = (char *) calloc(constants::BLOCK_SIZE, sizeof(char));
            int flag = read(buf, readRequest->offset(), constants::BLOCK_SIZE);
            if (flag == -1)
                return Status::CANCELLED;
            readResponse->set_data(buf);
            delete[] buf;
        }
        return Status::OK;
    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("Starting primary server write");
            BlockState state = BlockState::DISK;
            Info info = {state, writeRequest->data_length(), writeRequest->data()};
            temp_data[(int)writeRequest->offset()] = &info;
            if (backup_state == BackupState::ALIVE) {
                ClientContext context;
                ds::AckResponse ackResponse;
                LOG_DEBUG_MSG("sending read to backup");
                Status status = stub_->s_write(&context, *writeRequest, &ackResponse);
                LOG_DEBUG_MSG("back from backup");
            }
            LOG_DEBUG_MSG("write from map to file");
            int flag = write(writeRequest->data().c_str(), writeRequest->offset(), writeRequest->data_length());
            if (flag == -1)
                return Status::CANCELLED;
            writeResponse->set_bytes_written(writeRequest->data_length());
            if (backup_state == BackupState::ALIVE) {
                LOG_DEBUG_MSG("commit to backup");
                ClientContext context;
                ds::CommitRequest commitRequest;
                commitRequest.set_offset(writeRequest->offset());
                ds::AckResponse ackResponse;
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
            temp_data[(int) writeRequest->offset()] = &info;
        } else {
            std::cout << __LINE__ << "calling s_write at backup?\n" << std::flush;
        }
        return Status::OK;
    }

    Status s_commit(ServerContext *context, const ds::CommitRequest *commitRequest,
                    ds::AckResponse *ackResponse) {
        LOG_DEBUG_MSG("calling commit on backup");
        BlockState diskState = BlockState::DISK;
        Info *info = temp_data[(int)commitRequest->offset()];
        write(info->data.c_str(), commitRequest->offset(), info->length);
//        int bytes = pwrite(fd, info->data.c_str(), info->length, commitRequest->offset());
        temp_data.erase((int)commitRequest->offset());
        return Status::OK;
    }
};

int main(int argc, char *argv[]) {
    LOG_DEBUG_MSG("Starting primary");
    std::string server_address("0.0.0.0:50052");
    gRPCServiceImpl service(grpc::CreateChannel("localhost:50053",
        grpc::InsecureChannelCredentials()), argv[1]);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.SetMaxSendMessageSize(INT_MAX);
    builder.SetMaxReceiveMessageSize(INT_MAX);
    builder.SetMaxMessageSize(INT_MAX);
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}
