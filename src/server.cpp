#include "ds.grpc.pb.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <unistd.h>
#include <future>
#include <threads>

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
    enum class BlockState{ DISK, LOCKED, MEMORY};
    typedef struct {
        BlockState state;
        int length;
        std::string data;
    } Info;
    int fd;
    enum class BackupState {ALIVE, DEAD, REINTEGRATE};
    std::atomic<BackupState> backup_state;
    enum class ServerState: int32_t { PRIMARY = 0, BACKUP };
    std::atomic<ServerState> current_server_state_;
    std::unique_ptr <gRPCService::Stub> stub_;
    std::unordered_map<int, Info*> temp_data;
    std::unordered_map<int, Info*> mem_data;
    std::mutex reintegration_lock;
//    std::vector<std::future<Status>> pending_futures;

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

    explicit gRPCServiceImpl(std::shared_ptr<Channel> channel,
                             const std::string filename, bool primary = true) :
            stub_(gRPCService::NewStub(channel)){
        per_block_locks.resize(TOTAL_BLOCKS);
        current_server_state_ = (primary) ? ServerState::PRIMARY : ServerState::BACKUP;
        if (!primary) {
            secondary_reintegrate();
        }
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
        backup_state = BackupState::ALIVE;
        current_server_state_ = ServerState::PRIMARY;
    }

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
                  ds::ReadResponse *readResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            std::cout << __LINE__ << "\n";
            char *buf = (char *) calloc(constants::BLOCK_SIZE, sizeof(char));
            int bytes_read = pread(fd, buf, constants::BLOCK_SIZE, readRequest->offset());
            LOG_DEBUG_MSG(bytes_read, " bytes read");
            readResponse->set_data(buf);
            delete[] buf;
        } else {
            wait_before_read();
            std::cout << __LINE__ << "\n";
            char* buf = (char*) calloc(constants::BLOCK_SIZE, sizeof(char));
            lseek(fd, readRequest->offset(), SEEK_SET);
            int bytes_read = read(fd, buf, constants::BLOCK_SIZE);
            std::cout << bytes_read << " bytes read\n";
            readResponse->set_data(buf);
            delete[] buf;
        }
        return Status::OK;
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

    void get_write_locks(ServerContext *context, const ds::WriteRequest *writeRequest) {
        reintegration_lock.lock();
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

    void release_write_locks(ServerContext *context, const ds::WriteRequest *writeRequest) {
        vector<int> blocks = get_blocks_involved(writeRequest->offset, writeRequest->data_length);
        for (const int &block: blocks) {
            per_block_locks[block].release();
        }
        reintegration_lock.unlock();
    }

    Status p_reintegrate(ServerContext *context, const ds::ReintegrateRequest* reintegrateRequest,
                         ds::ReintegrateResponse* reintegrateResponse) {

        assert_msg(current_server_state_ != ServerState::PRIMARY, "Reintegration called on backup");
        this->backup_state = BackupState::REINTEGRATE;
        // return the entries in temp_data that are in disk state.
        // opt_todo: stream the entries instead of returning at once
        for (auto it: temp_data) {
            Info *info = it.second;
            if (info->state == BlockState::DISK) {
                int *offset = reintegrateResponse->add_offsets();
                *offset = it.first;
                int *data_length = reintegrateResponse->add_data_lengths();
                *data_length = info->length;
                string *data = reintegrateResponse->add_data();
                *data = info->data;
            }
        }
        return Status::OK;
    }

    Status p_reintegrate_phase_two(ServerContext *context, const ds::ReintegrateRequest* reintegrateRequest,
                         ds::ReintegrateResponse* reintegrateResponse) {

        assert_msg(current_server_state_ != ServerState::PRIMARY, "Reintegration called on backup");

        // pause all writes for now!
        reintegration_lock.lock();

        // return the entries in temp_data that are in MEMORY state.
        // opt_todo: stream the entries instead of returning at once
        for (auto it: temp_data) {
            Info *info = it.second;
            if (info->state == BlockState::MEMORY) {
                int *offset = reintegrateResponse->add_offsets();
                *offset = it.first;
                int *data_length = reintegrateResponse->add_data_lengths();
                *data_length = info->length;
                string *data = reintegrateResponse->add_data();
                *data = info->data;
            }
        }
        return Status::OK;
    }

    Status p_reintegration_complete(ServerContext *context, const ds::ReintegrateRequest* reintegrateRequest,
                                    ds::ReintegrateResponse* reintegrateResponse) {
        this->backup_state = BackupState::ALIVE;
        reintegration_lock.unlock();
        return Status::OK;
    }

    void secondary_reintegrate() {
        ClientContext* context;
        ReintegrationRequest* reintegration_request = new ReintegrationRequest;
        ReintegrationResponse* reintegration_response = new ReintegrationResponse;
        Status status = stub_->p_reintegrate(context, reintegration_request, reintegration_response);

        // write all missing writes in the backup
        for (int i = 0; i < reintegration_response.data_size(); i++) {
            pwrite(fd, &reintegration_response->data(i), reintegration_response->data_length(i),
                   reintegration_response->offsets(i));
        }

        // get memory based writes
        reintegration_response->clear_data();
        reintegration_response->clear_data_lengths();
        reintegration_response->clear_offsets();
        status = stub_->p_reintegrate_phase_two(context, reintegration_request, reintegration_response);

        for (int i = 0; i < reintegration_response.data_size(); i++) {
            pwrite(fd, &reintegration_response->data(i), reintegration_response->data_length(i),
                   reintegration_response->offsets(i));
        }

        // notify primary that reintegration is complete
        reintegration_response->clear_data();
        reintegration_response->clear_data_lengths();
        reintegration_response->clear_offsets();
        status = stub_->p_reintegration_complete(context, reintegration_request, reintegration_response);

    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("Starting primary server write");
            get_write_locks(writeRequest->offset);
//            for (int i = 0; i < pending_futures.size(); i++) {
//                if (pending_futures[i].valid()) {
//                        int address = pending_futures[i].get();
//                        temp_data.erase(address);
//                }
//            }

            BlockState state = (backup_state == BackupState::REINTEGRATE) ? BlockState::MEMORY : BlockState::DISK;
            Info info = {state, writeRequest->data_length(), writeRequest->data()};
            temp_data[(int)writeRequest->offset()] = &info;

            if (backup_state == BackupState::ALIVE) {
                ClientContext context;
                ds::AckResponse ackResponse;
                LOG_DEBUG_MSG("sending write to backup");
                Status status = stub_->s_write(&context, *writeRequest, &ackResponse);
                LOG_DEBUG_MSG("back from backup");
            }
            LOG_DEBUG_MSG("write from map to file");
            int bytes = pwrite(fd, &writeRequest->data(), writeRequest->data_length(), writeRequest->offset());
            writeResponse->set_bytes_written(bytes);
            if (backup_state == BackupState::ALIVE) {
                LOG_DEBUG_MSG("commit to backup");
                ClientContext context;
                ds::CommitRequest commitRequest;
                commitRequest.set_offset(writeRequest->offset());
                ds::AckResponse ackResponse;
//                std::future<Status> f = std::async(std::launch::async,
//                    stub_->s_commit, &context, commitRequest, &ackResponse);
//                Status status = stub_->s_commit(&context, commitRequest, &ackResponse);
//                pending_futures.push_back(std::move(f));
                LOG_DEBUG_MSG("committed to backup");
            }
            release_write_locks(writeRequest);
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
        int bytes = pwrite(fd, info->data.c_str(), info->length, commitRequest->offset());
        temp_data.erase((int)commitRequest->offset());
        return Status::OK;
    }
};

//gRPCServiceImpl::~gRPCServiceImpl() {
//    LOG_MSG_DEBUG("Calling destructor\n");
//    close(fd);
//}

int main(int argc, char *argv[]) {
    LOG_DEBUG_MSG("Starting primary");
    std::string server_address("0.0.0.0:50052");
    gRPCServiceImpl service(grpc::CreateChannel("localhost:50053",
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
