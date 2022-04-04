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
#include <deque>
#include <future>
#include <thread>
#include <shared_mutex>

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

struct MyLock {

    std::atomic<int> shared_lock_acquired;
    std::atomic<int> lock_acquired;

    MyLock() {
        shared_lock_acquired = 0;
        lock_acquired = 0;
    }
    void lock_shared() {
//        std::cerr << " + " << &m << "got shared_lock\n";
        while(true) {
            if (lock_acquired == 1) {
               usleep(1);
            }
            if (!lock_acquired) {
                shared_lock_acquired += 1 ;
                return;
            }
        }
    }
    void unlock_shared() {
        shared_lock_acquired -= 1;
        ASS(shared_lock_acquired < 0, "SHARED_LOCK count can't be negative");
    }
    void lock() {
        while(1) {
            if (shared_lock_acquired == 0) {
                if (lock_acquired == 0) {
                    lock_acquired = 1;
                    return;
                }
            }
            usleep(1);
        }
    }
    void unlock() {
        if (lock_acquired) {
            lock_acquired = 0;
        }
    }
    bool try_lock() {
        bool ret;
        if (lock_acquired) {
            ret = false;
        } else {
            lock_acquired = 1;
            ret = true;
        }

//        std::cerr << " ? " << &m << "try_lock" << ret <<"\n";
        return ret;
    }
};

class gRPCServiceImpl final : public gRPCService::Service {
private:
    enum class BlockState{DISK, LOCKED, MEMORY};
    struct Info {
        BlockState state;
        int length;
        std::string data;
        Info(BlockState& s, int l, std::string d): state(s), length(l), data(std::move(d)){}
    };
    std::string filename;
    enum class BackupState {ALIVE, DEAD, REINTEGRATION};
    std::atomic<BackupState> backup_state;
    enum class ServerState: int32_t { PRIMARY = 0, BACKUP };
    std::atomic<ServerState> current_server_state_;
    std::unique_ptr <gRPCService::Stub> stub_;
    std::unordered_map<int, std::unique_ptr<Info>> temp_data;
    using fut_t = std::future<std::optional<int>>;
    std::deque<fut_t> pending_futures;
    MyLock reintegration_lock;
    std::vector<std::mutex> per_block_locks;
    std::fstream file;
    int fd;
public:
    void create_file(const std::string filename) {
        fd = ::open(filename.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        if (fd == -1) {
            LOG_ERR_MSG("unable to open fd for: ", filename);
        }
    }

    int write(const char* buf, int address, int size) {
        LOG_DEBUG_MSG("writing @", address, ": ", std::string(buf, size));
        const int written_b = ::pwrite(fd, buf, size, address);
        if (written_b == -1) {
            LOG_ERR_MSG("write failed @ ", address, " ", errno);
        }
        return written_b;
    }

    int read(char* buf, int address, int size) {
        const int read_b = ::pread(fd, buf, size, address);
        if (read_b == -1) {
            LOG_ERR_MSG("read failed @ ", address, " ", errno);
        }
        LOG_DEBUG_MSG("reading @", address, ":", size, " -> ", std::string(buf, read_b));
        return read_b;
    }

    auto get_server_state() const {
        return current_server_state_.load(std::memory_order_relaxed);
    }

    void set_server_state(ServerState serverState) {
        current_server_state_.store(serverState);
    }

    void set_backup_state(BackupState backupState) {
        backup_state.store(backupState);
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

    explicit gRPCServiceImpl(
            std::shared_ptr<Channel> channel, const std::string filename) :
            stub_(gRPCService::NewStub(channel)) {
        std::vector<std::mutex> new_locks(constants::BLOCK_SIZE);
        per_block_locks.swap(new_locks);
//        current_server_state_ = (primary)? ServerState::PRIMARY : ServerState::BACKUP;
        this->filename = filename;
        create_file(filename);
        LOG_DEBUG_MSG("Filename ", this->filename, " f:", filename);
//        if (!primary) {
//            LOG_DEBUG_MSG("reintegration started at backup");
//            secondary_reintegration();
//            backup_state.store(BackupState::ALIVE);
//        }
    }

    void wait_before_read(const ds::ReadRequest* readRequest) {
//        ASS(current_server_state_ == ServerState::PRIMARY,
//            "waiting for reads in primary shouldn't happen");
        std::vector<int> blocks = get_blocks_involved(
                readRequest->address(), constants::BLOCK_SIZE);
        // change this to get signaled when the entry is removed
        // from the map (write to that block is complete)
        bool can_read_all = false;
        if (readRequest->address() == 6) {
            LOG_DEBUG_MSG("backup request to read a block");
        }
        while(!can_read_all) {
            can_read_all = true;
            for (const int &b: blocks) {
                if (temp_data.count(b) && temp_data[b]->state == BlockState::LOCKED) {
                    can_read_all = false;
                    break;
                }
            }
            if (!can_read_all) {
                LOG_DEBUG_MSG("read waiting on locked: ", readRequest->address());
                std::this_thread::sleep_for(std::chrono::nanoseconds((int)1e5));
            }
        }
    }

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
        ds::ReadResponse *readResponse) {
        if (current_server_state_ == ServerState::BACKUP) {
            if (readRequest->address() == 6) {
                LOG_DEBUG_MSG("backup request to read a locked block");
            }
            wait_before_read(readRequest);
        }
        if (get_server_state() == ServerState::BACKUP)
            LOG_DEBUG_MSG("reading from backup");
        else LOG_DEBUG_MSG("reading from primary");
        int buf_size = readRequest->data_length();
        auto buf = std::make_unique<char[]>(buf_size);
        ::bzero(buf.get(), buf_size);
        int bytes_read = read(buf.get(), readRequest->address(), buf_size);
        LOG_DEBUG_MSG(std::string(buf.get(), bytes_read), " bytes read");
        readResponse->set_data(buf.get(), bytes_read);
        return Status::OK;
    }

    Status hb_check(ServerContext *context, const ds::HBRequest *request,ds::HBResponse *response) {
//        LOG_DEBUG_MSG("temp_data_size:", std::to_string(temp_data.size()));
//        if (temp_data.size() > 0)
//            LOG_DEBUG_MSG("info->state", temp_data[0]->state == BlockState::DISK ? "DISK" : "NOT_DISK");

        return Status::OK;
    }

    Status hb_tell(ServerContext *context, const ds::HBRequest *request,ds::HBResponse *response) {
//        LOG_DEBUG_MSG("temp_data_size:", std::to_string(temp_data.size()));
//        if (temp_data.size() > 0)
//            LOG_DEBUG_MSG("info->state", temp_data[0]->state == BlockState::DISK ? "DISK" : "NOT_DISK");

//        if (request->has_is_primary()) {
            if (request->is_primary()) {
                LOG_DEBUG_MSG("becoming primary");
                set_server_state(ServerState::PRIMARY);
            } else {
                LOG_DEBUG_MSG("becoming secondary");
                set_server_state(ServerState::BACKUP);
                LOG_DEBUG_MSG("reintegration started at backup");
                // start in async
            secondary_reintegration();
//            backup_state.store(BackupState::ALIVE);
            }
//        }
//        if (request->has_sec_alive()) {
            if (request->sec_alive()) {
                LOG_DEBUG_MSG("setting backup alive");
                set_backup_state(BackupState::ALIVE);
            } else {
                LOG_DEBUG_MSG("setting backup dead");
                set_backup_state(BackupState::DEAD);
            }
//        }
        return Status::OK;
    }

    // Returns the block indices for the address and data_length.
    // In this case return vector size is at most 2
    std::vector<int> get_blocks_involved(const int address, const int data_length) {
        LOG_DEBUG_MSG(data_length);
        int first_block = address / constants::BLOCK_SIZE;
        int end_of_first_block = (first_block + 1) * constants::BLOCK_SIZE - 1;
        int first_block_size_left = end_of_first_block - address + 1;
        std::vector<int> blocks_involved;
        blocks_involved.push_back(first_block);
        if (data_length > first_block_size_left) {
            blocks_involved.push_back(first_block + 1);
            LOG_DEBUG_MSG("second block added ", first_block+1);
        }
        return blocks_involved;
    }

    void get_write_locks(const ds::WriteRequest *writeRequest) {
        auto blocks = get_blocks_involved(
                writeRequest->address(), writeRequest->data_length());
        LOG_DEBUG_MSG("Trying to get re_int lock");
        LOG_DEBUG_MSG("TAKING SHARED LOCK");
        reintegration_lock.lock_shared();

        for (const int &block: blocks) {
            per_block_locks[block].lock();
        }
        for (auto block_num : blocks) {
            LOG_DEBUG_MSG("locks acquired on ", block_num);
        }
    }

    void release_write_locks(const ds::WriteRequest *writeRequest) {
        auto blocks = get_blocks_involved(
                writeRequest->address(), writeRequest->data_length());
        for (const int &block: blocks) {
            per_block_locks[block].unlock();
        }
        LOG_DEBUG_MSG("UNLOKING SHARED LOCK");
        reintegration_lock.unlock_shared();
        for (auto block_num : blocks) {
            LOG_DEBUG_MSG("locks released on ", block_num);
        }
    }

    Status p_reintegration(ServerContext *context,
                           const ds::ReintegrationRequest* reintegrationRequest,
                           ds::ReintegrationResponse* reintegrationResponse) {
//        assert_msg(current_server_state_ != ServerState::PRIMARY,
//                   "Reintegration called on backup");
        LOG_DEBUG_MSG("reintegration called at primary");
        this->backup_state = BackupState::REINTEGRATION;
        // return the entries in temp_data that are in disk state.
        // opt_todo: stream the entries instead of returning at once
        int c = 0; // debug
        LOG_DEBUG_MSG("temp_data_size:" + std::to_string(temp_data.size()));
        for (auto& it: temp_data) {
            auto& info = it.second;
            LOG_DEBUG_MSG("info ->", "(state) ", (info->state == BlockState::DISK ? "disk" : "not disk"));
            if (info->state == BlockState::DISK) {
                LOG_DEBUG_MSG("inside if1");
                int address = it.first;
                reintegrationResponse->add_addresses(address);
                LOG_DEBUG_MSG("inside if2");
                reintegrationResponse->add_data_lengths(info->length);
                LOG_DEBUG_MSG("inside if3");
                reintegrationResponse->add_data(info->data);
                c++;
                LOG_DEBUG_MSG("inside if4");
            }
        }
        LOG_DEBUG_MSG("records in disk state:", std::to_string(c));
        return Status::OK;
    }

    Status p_reintegration_phase_two(ServerContext *context,
                                     const ds::ReintegrationRequest* reintegrationRequest,
                                     ds::ReintegrationResponse* reintegrationResponse) {
//        assert_msg(current_server_state_ != ServerState::PRIMARY,
//                   "Reintegration called on backup");
        LOG_DEBUG_MSG("reintegration phase 2 started, stalling all writes");
        // pause all writes for now!
        LOG_DEBUG_MSG("TAKING LOCK");
        reintegration_lock.lock();
        LOG_DEBUG_MSG("size of temp:", temp_data.size());

        // return the entries in temp_data that are in MEMORY state.
        // opt_todo: stream the entries instead of returning at once
        for (auto& it: temp_data) {
            auto& info = it.second;
            if (info->state == BlockState::MEMORY) {
                reintegrationResponse->add_addresses(it.first);
                reintegrationResponse->add_data_lengths(info->length);
                reintegrationResponse->add_data(info->data);
            }
        }
        return Status::OK;
    }

    Status p_reintegration_complete(ServerContext *context,
                                    const ds::ReintegrationRequest* reintegrationRequest,
                                    ds::ReintegrationResponse* reintegrationResponse) {
//        assert_msg(current_server_state_ != ServerState::PRIMARY,
//                   "Reintegration called on backup");
        LOG_DEBUG_MSG("reintegraton complete lock unlocked");
        set_backup_state(BackupState::ALIVE);
        LOG_DEBUG_MSG("RELEASING LOCK");
        temp_data.clear();
        LOG_DEBUG_MSG("Size of temp_data", temp_data.size());
        reintegration_lock.unlock();
        if (reintegration_lock.try_lock()) {
            LOG_DEBUG_MSG("lock status:", "lock aquired");
            reintegration_lock.unlock();
            LOG_DEBUG_MSG("lock status:", "lock released");
        } else {
            LOG_DEBUG_MSG("lock status:", "lock not released??");
        }

        return Status::OK;
    }

    void secondary_reintegration() {
        LOG_DEBUG_MSG("here");


//        assert_msg(current_server_state_ != ServerState::BACKUP,
//                   "Reintegration called on primary");
        LOG_DEBUG_MSG("here");
        ClientContext context;
        ds::ReintegrationRequest reintegration_request;
        ds::ReintegrationResponse reintegration_response;
        LOG_DEBUG_MSG("sending reintegration request to primary");
        Status status = stub_->p_reintegration(
                &context, reintegration_request, &reintegration_response);
        if (!status.ok()) {
            LOG_DEBUG_MSG("error: ", status.error_code(), status.error_message());
        }

        if (std::getenv("SERVER_CRASH_AFTER_REINTEGRATION_PHASE_1")) {
            LOG_DEBUG_MSG("Exiting after reintegration phase 1 completed");
            exit(1);
        }

        LOG_DEBUG_MSG("received reintegration response at secondary");
        // write all missing writes in the backup

        for (int i = 0; i < reintegration_response.data_size(); i++) {
            write(reintegration_response.data(i).c_str(),
                   reintegration_response.addresses(i),
                  reintegration_response.data_lengths(i));
        }
        LOG_DEBUG_MSG("wrote " + std::to_string(reintegration_response.data_size()) + " DISK records");
        // get memory based writes
        reintegration_response.clear_data();
        reintegration_response.clear_data_lengths();
        reintegration_response.clear_addresses();
        ClientContext context1;
        LOG_DEBUG_MSG("sending reintegration phase 2 request to primary");
        status = stub_->p_reintegration_phase_two(
                &context1, reintegration_request, &reintegration_response);
        if (!status.ok()) {
            LOG_DEBUG_MSG("error: ", status.error_code(), status.error_message());
        }

        if (std::getenv("SERVER_CRASH_AFTER_REINTEGRATION_PHASE_2")) {
            LOG_ERR_MSG("Exiting after reintegration phase 2 completed\n");
            exit(1);
        }

        LOG_DEBUG_MSG("received reintegration phase 2 response at secondary");
        for (int i = 0; i < reintegration_response.data_size(); i++) {
            write(reintegration_response.data(i).c_str(),
                    reintegration_response.addresses(i),
                    reintegration_response.data_lengths(i));
        }
        LOG_DEBUG_MSG("wrote " + std::to_string(reintegration_response.data_size()) + " MEM records");
        // notify primary that reintegration is complete
        reintegration_response.clear_data();
        reintegration_response.clear_data_lengths();
        reintegration_response.clear_addresses();
        ClientContext context2;
        status = stub_->p_reintegration_complete(
            &context2, reintegration_request, &reintegration_response);
        if (!status.ok()) {
            LOG_DEBUG_MSG("error: ", status.error_code(), status.error_message());
        }

        //set_backup_state(BackupState::ALIVE);
        LOG_DEBUG_MSG("reintegration complete");
    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        LOG_DEBUG_MSG("here");
//        assert_msg(current_server_state_ != ServerState::PRIMARY,
//                   "Reintegration called on backup");
        if (current_server_state_ != ServerState::PRIMARY) {
            return Status::CANCELLED;
        }
        LOG_DEBUG_MSG("Starting primary server write");
        while (pending_futures.size()) {
            LOG_DEBUG_MSG(pending_futures.size(), " pending futures found");
            auto& pf = pending_futures.front();
            if (pf.valid()) {
                LOG_DEBUG_MSG("future ready, decreasing size of map");
                const auto addr = pf.get();
                if (addr) {
                    LOG_DEBUG_MSG("address found in optional int", *addr);
                    temp_data.erase(addr.value());
                }
                pending_futures.pop_front();
                LOG_DEBUG_MSG("temp_data size:", temp_data.size());
            }
        }
        get_write_locks(writeRequest);
        BlockState state = (backup_state == BackupState::REINTEGRATION) ? BlockState::MEMORY : BlockState::DISK;
        LOG_DEBUG_MSG("Backup state:", backup_state == BackupState::REINTEGRATION ? "r" : "not r");
        LOG_DEBUG_MSG("wrtiing in primary with state ", ((state == BlockState::DISK) ? "DISK" : "MEM"));
//        Info info = {};
        // TODO: make map thread safe
        temp_data[(int)writeRequest->address()] = std::make_unique<Info>(state, writeRequest->data_length(), writeRequest->data());

        LOG_DEBUG_MSG("temp_data size:" + std::to_string(temp_data.size()));
        if (backup_state == BackupState::ALIVE) {
            ClientContext context;
            ds::AckResponse ackResponse;
            LOG_DEBUG_MSG("sending write to backup");
            Status status = stub_->s_write(&context, *writeRequest, &ackResponse);
            if (!status.ok()) {
                LOG_DEBUG_MSG("error ", status.error_code(), status.error_message());
            }

            if (std::getenv("SERVER_CRASH_AFTER_BACKUP_WRITE")) {
                LOG_ERR_MSG("Exiting after sending write to backup\n");
                exit(1);
            }

            LOG_DEBUG_MSG("back from backup");
        }
        LOG_DEBUG_MSG("write from map to file:", writeRequest->data());
        int flag = write(writeRequest->data().c_str(), writeRequest->address(),
                         writeRequest->data_length());
        release_write_locks(writeRequest);
        if (flag == -1) {
            throw std::logic_error("unlock at backup!!");
            return Status::CANCELLED;
        }
        writeResponse->set_bytes_written(writeRequest->data_length());

        if (backup_state == BackupState::ALIVE) {
            LOG_DEBUG_MSG("commit to backup");
            ClientContext context;
            ds::CommitRequest commitRequest;
            const int waddr = writeRequest->address();
            LOG_DEBUG_MSG("waddr to ret = ", waddr);
            commitRequest.set_address(waddr);

            fut_t f = std::async(std::launch::async,
                [&, waddr]() -> std::optional<int> {
                    ClientContext context;
                    ds::CommitRequest commitRequest;
                    commitRequest.set_address(waddr);
                    ds::AckResponse ackResponse;
                    if ((stub_->s_commit(&context, commitRequest, &ackResponse)).ok()) {
                        LOG_DEBUG_MSG("waddr to ret = ", waddr);
                        return waddr;
                    }
                    return std::nullopt;
            });
            
            pending_futures.push_back(std::move(f));
            if (std::getenv("SERVER_CRASH_AFTER_SENDING_BACKUP_COMMIT")) {
                LOG_ERR_MSG("Exiting after sending commit to backup\n");
                exit(1);
            }
            LOG_DEBUG_MSG("committed to backup");
        }
        return Status::OK;
    }

    Status s_write(ServerContext *context, const ds::WriteRequest *writeRequest,
        ds::AckResponse *ackResponse) {
//        assert_msg(current_server_state_ != ServerState::BACKUP,
//                   "Reintegration called on backup");
        if (current_server_state_ == ServerState::BACKUP) {
            LOG_DEBUG_MSG("Starting backup server write");
            BlockState state = BlockState::LOCKED;
            temp_data[(int) writeRequest->address()] = std::make_unique<Info>(state, writeRequest->data_length(), writeRequest->data());
            LOG_DEBUG_MSG("Pausing write in backup server");
            int debug_integer;
//            sleep(10);
            LOG_DEBUG_MSG("waking from sloeep");
        } else {
            LOG_DEBUG_MSG("calling s_write at primary whyyy?");
        }
        return Status::OK;
    }

    Status s_commit(ServerContext *context, const ds::CommitRequest *commitRequest,
        ds::AckResponse *ackResponse) {
//        assert_msg(current_server_state_ != ServerState::BACKUP,
//                   "Reintegration called on backup");
        LOG_DEBUG_MSG("calling commit on backup");
        auto& info = temp_data[(int)commitRequest->address()];
        write(info->data.c_str(), commitRequest->address(), info->length);
        temp_data.erase((int)commitRequest->address());
        return Status::OK;
    }

    ~gRPCServiceImpl() {
        LOG_DEBUG_MSG("Calling destructor");
        ::close(fd);
    }
};

int main(int argc, char *argv[]) {
    if(argc < 7) {
        printf("Usage : ./server -self <myIP:port> -other <otherIP:port> -datafile <datafile>\n");
        return 0;
    }

    std::string server_address{"localhost:60052"}, other_address{"localhost:60053"}, datafile{"data"};
    for(int i = 1; i < argc - 1; ++i) {
        if(!strcmp(argv[i], "-self")) {
            server_address = std::string{argv[i+1]};
        } else if (!strcmp(argv[i], "-other")) {
            other_address = std::string{argv[i+1]};
        } else if (!strcmp(argv[i], "-datafile")) {
            datafile = std::string{argv[i+1]};
        }
    }

    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, constants::MAX_RECONN_TIMEOUT);

    gRPCServiceImpl service(grpc::CreateCustomChannel(other_address,
        grpc::InsecureChannelCredentials(), args), datafile);
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