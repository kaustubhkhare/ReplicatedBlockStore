#include "ds.grpc.pb.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <memory>
#include <future>
#include <thread>

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
    std::string filename;
    enum class BackupState {ALIVE, DEAD, REINTEGRATION};
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
    void create_file(const std::string filename) {
        std::fstream stream;
        stream.open(filename, std::fstream::in | std::fstream::out | std::fstream::app);

        if (!stream.is_open()) {
            std::cout << "File doesn't exist. Creating file.";
            stream.open(filename,  std::fstream::in | std::fstream::out | std::fstream::trunc);
            stream.close();
        }
    }

    int write(const char* buf, int address, int size) {
        std::fstream outfile;
        LOG_DEBUG_MSG("Opening file ", filename, " for writing");
        outfile.open(filename);
        outfile.seekp(address);
        outfile.write(buf, size);
        outfile.close();

        return size;
    }

    int read(char* buf, int address, int size) {
        std::ifstream infile;
        LOG_DEBUG_MSG("Opening file ", filename, " for reading");
        infile.open(filename, std::ios::binary);
        infile.seekg(address);
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
        per_block_locks.resize(constants::TOTAL_BLOCKS);
        current_server_state_ = (primary)? ServerState::PRIMARY : ServerState::BACKUP;
        this->filename = filename;
        create_file(filename);
        LOG_DEBUG_MSG("Filename ", this->filename, " f:", filename);
        backup_state = BackupState::ALIVE;
        current_server_state_ = primary ? ServerState::PRIMARY : ServerState::BACKUP;
        if (!primary) {
            secondary_reintegration();
        }
        LOG_DEBUG_MSG("constructor called");
    }

    void wait_before_read(const ds::ReadRequest* readRequest) {
        ASS(current_server_state_ == ServerState::PRIMARY, "waiting for reads in primary shouldn't happen");
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

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
        ds::ReadResponse *readResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("reading from primary");
            char *buf = (char *) calloc(constants::BLOCK_SIZE, sizeof(char));
            int bytes_read = read(buf, readRequest->address(), constants::BLOCK_SIZE);
            LOG_DEBUG_MSG(bytes_read, " bytes read");
            readResponse->set_data(buf);
            delete[] buf;
        } else {
            wait_before_read(readRequest);
            LOG_DEBUG_MSG("reading from backup");
            char* buf = (char*) calloc(constants::BLOCK_SIZE, sizeof(char));
            int flag = read(buf, readRequest->address(), constants::BLOCK_SIZE);
            if (flag == -1)
                return Status::CANCELLED;
            readResponse->set_data(buf);
            delete[] buf;
        }
        return Status::OK;
    }
    // Returns the block indices for the address and data_length. In this case return vector size is at most 2
    std::vector<int> get_blocks_involved(const int address, const int data_length) {
        int first_block = address / BLOCK_SIZE;
        int end_of_first_block = first_block + BLOCK_SIZE - 1,
            first_block_size_left = end_of_first_block - first_block * BLOCK_SIZE;
        std::vector blocks_involved;
        blocks_involved.push_back(first_block);
        if (data_length > first_block_size_left) {
            blocks_involved.push_back(first_block + 1);
        }
        return blocks_involved;
    }

    Status hb_check(ServerContext *context, const ds::HBRequest *request, ds::HBResponse *response) {
        if (request->is_primary()) {
            LOG_DEBUG_MSG("I'm primary");
        } else {
            LOG_DEBUG_MSG("I'm secondary");
        }

        return Status::OK;
    }

    void get_write_locks(ServerContext *context, const ds::WriteRequest *writeRequest) {
        reintegration_lock.lock();
        std::vector<int> blocks = get_blocks_involved(writeRequest->address, writeRequest->data_length);
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
        std::vector<int> blocks = get_blocks_involved(writeRequest->address, writeRequest->data_length);
        for (const int &block: blocks) {
            per_block_locks[block].release();
        }
        reintegration_lock.unlock();
    }

    Status p_reintegration(ServerContext *context, const ds::ReintegrationRequest* reintegrationRequest,
                         ds::ReintegrationResponse* reintegrationResponse) {

        assert_msg(current_server_state_ != ServerState::PRIMARY, "Reintegration called on backup");
        this->backup_state = BackupState::REINTEGRATION;
        // return the entries in temp_data that are in disk state.
        // opt_todo: stream the entries instead of returning at once
        for (auto it: temp_data) {
            Info *info = it.second;
            if (info->state == BlockState::DISK) {
                int *address = reintegrationResponse->add_addressss();
                *address = it.first;
                int *data_length = reintegrationResponse->add_data_lengths();
                *data_length = info->length;
                string *data = reintegrationResponse->add_data();
                *data = info->data;
            }
        }
        return Status::OK;
    }

    Status p_reintegration_phase_two(ServerContext *context, const ds::ReintegrationRequest* reintegrationRequest,
                         ds::ReintegrationResponse* reintegrationResponse) {

        assert_msg(current_server_state_ != ServerState::PRIMARY, "Reintegration called on backup");

        // pause all writes for now!
        reintegration_lock.lock();

        // return the entries in temp_data that are in MEMORY state.
        // opt_todo: stream the entries instead of returning at once
        for (auto it: temp_data) {
            Info *info = it.second;
            if (info->state == BlockState::MEMORY) {
                int *address = reintegrationResponse->add_addresses();
                *address = it.first;
                int *data_length = reintegrationResponse->add_data_lengths();
                *data_length = info->length;
                string *data = reintegrationResponse->add_data();
                *data = info->data;
            }
        }
        return Status::OK;
    }

    Status p_reintegration_complete(ServerContext *context, const ds::ReintegrationRequest* reintegrationRequest,
                                    ds::ReintegrationResponse* reintegrationResponse) {
        this->backup_state = BackupState::ALIVE;
        reintegration_lock.unlock();
        return Status::OK;
    }

    void secondary_reintegration() {
        ClientContext* context;
        ReintegrationRequest* reintegration_request = new ReintegrationRequest;
        ReintegrationResponse* reintegration_response = new ReintegrationResponse;
        Status status = stub_->p_reintegration(context, reintegration_request, reintegration_response);

        // write all missing writes in the backup
        for (int i = 0; i < reintegration_response.data_size(); i++) {
            write(&reintegration_response->data(i),
                   reintegration_response->addresses(i),
                  reintegration_response->data_length(i));
        }

        // get memory based writes
        reintegration_response->clear_data();
        reintegration_response->clear_data_lengths();
        reintegration_response->clear_addresses();
        status = stub_->p_reintegration_phase_two(context, reintegration_request, reintegration_response);

        for (int i = 0; i < reintegration_response.data_size(); i++) {
            write(&reintegration_response->data(i),
                    reintegration_response->addresses(i),
                    reintegration_response->data_length(i));
        }

        // notify primary that reintegration is complete
        reintegration_response->clear_data();
        reintegration_response->clear_data_lengths();
        reintegration_response->clear_addresses();
        status = stub_->p_reintegration_complete(context, reintegration_request, reintegration_response);

    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        if (current_server_state_ == ServerState::PRIMARY) {
            LOG_DEBUG_MSG("Starting primary server write");
            get_write_locks(writeRequest->address);
//            for (int i = 0; i < pending_futures.size(); i++) {
//                if (pending_futures[i].valid()) {
//                        int address = pending_futures[i].get();
//                        temp_data.erase(address);
//                }
//            }

            BlockState state = (backup_state == BackupState::REINTEGRATION) ? BlockState::MEMORY : BlockState::DISK;
            Info info = {state, writeRequest->data_length(), writeRequest->data()};
            temp_data[(int)writeRequest->address()] = &info;
            if (backup_state == BackupState::ALIVE) {
                ClientContext context;
                ds::AckResponse ackResponse;
                LOG_DEBUG_MSG("sending write to backup");
                Status status = stub_->s_write(&context, *writeRequest, &ackResponse);
                LOG_DEBUG_MSG("back from backup");
            }
            LOG_DEBUG_MSG("write from map to file");
            int flag = write(writeRequest->data().c_str(), writeRequest->address(), writeRequest->data_length());
            if (flag == -1)
                return Status::CANCELLED;
            writeResponse->set_bytes_written(writeRequest->data_length());
            if (backup_state == BackupState::ALIVE) {
                LOG_DEBUG_MSG("commit to backup");
                ClientContext context;
                ds::CommitRequest commitRequest;
                commitRequest.set_address(writeRequest->address());
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
            temp_data[(int) writeRequest->address()] = &info;
        } else {
            std::cout << __LINE__ << "calling s_write at backup?\n" << std::flush;
        }
        return Status::OK;
    }

    Status s_commit(ServerContext *context, const ds::CommitRequest *commitRequest,
                    ds::AckResponse *ackResponse) {
        LOG_DEBUG_MSG("calling commit on backup");
        BlockState diskState = BlockState::DISK;
        Info *info = temp_data[(int)commitRequest->address()];
        write(info->data.c_str(), commitRequest->address(), info->length);
        temp_data.erase((int)commitRequest->address());
        return Status::OK;
    }

    ~gRPCServiceImpl() {
        LOG_DEBUG_MSG("Calling destructor");
    }

};

int main(int argc, char *argv[]) {
    if(argc < 7) {
        printf("Usage : ./server -ip <ip> -port <port> -datafile <datafile>\n");
        return 0;
    }

    std::string ip{"0.0.0.0"}, port{"50052"}, datafile{"data"};
    for(int i = 1; i < argc - 1; ++i) {
        if(!strcmp(argv[i], "-ip")) {
            ip = std::string{argv[i+1]};
        } else if(!strcmp(argv[i], "-port")) {
            port = std::string{argv[i+1]};
        } else if (!strcmp(argv[i], "-datafile")) {
            datafile = std::string{argv[i+1]};
        }
    }

    LOG_DEBUG_MSG("Starting primary");
    std::string server_address(ip + ":" + port);
    gRPCServiceImpl service(grpc::CreateChannel("localhost:50053",
        grpc::InsecureChannelCredentials()), datafile);
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
