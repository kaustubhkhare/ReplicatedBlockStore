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

//class MyLock {
//    static std::mutex m;
//    unsigned int readers: 20;
//    unsigned int waiting_writers: 10;
//    unsigned int writer: 1;
//public:
//    MyLock() {
//        waiting_writers = readers = writer = 0;
//    }
//
//    bool unlocked_if_write_locked() {
//        std::lock_guard l(m);
//        if (writer != 0) {
//            assert(writer-- == 1);
//        }
//        LOG_DEBUG_MSG("unlocked EXCL lock");
//    }
//
//    void lock_shared() {
//        while (1) {
//            {
//                std::lock_guard l(m);
//                if (waiting_writers + writer == 0) {
//                    readers++;
//                    break;
//                }
//            }
//            usleep(10);
//            LOG_DEBUG_MSG("Can't get the shared lock?");
//        }
//    }
//    void unlock_shared() {
//        std::lock_guard l(m);
//        assert(writer == 0);
//        assert(readers-- > 0);
//    }
//    void lock() {
//        int expected = 0;
//        while (1) {
//            {
//                std::lock_guard l(m);
//                if (writer + readers + waiting_writers == expected) {
//                    writer = 1;
//                    break;
//                } else if (expected == 0) {
//                    waiting_writers++;
//                    expected++;
//                }
//            }
//            usleep(10);
//        }
//    }
//    void unlock() {
//        std::lock_guard l(m);
//        assert(writer-- == 1);
//    }
//    bool try_lock() {
//        std::lock_guard l(m);
//        if (writer + readers + waiting_writers == 0) {
//            writer = 1;
//            return true;
//        }
//        return false;
//    }
//};

struct MyLock {
    std::mutex m;
    void lock_shared() {
//        std::cerr << " + " << &m << "got shared_lock\n";
        m.lock();
    }
    void unlock_shared() {
//        std::cerr << " - " << &m << "rel shared_lock\n";
        m.unlock();
    }
    void lock() {
//        std::cerr << " + " << &m << "got lock\n";
        m.lock();
    }
    void unlock() {
//        std::cerr << " - " << &m << "rel lock\n";
        m.unlock();
    }
    bool try_lock() {
        const auto ret = m.try_lock();
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
    std::unordered_map<int, std::shared_ptr<Info>> temp_data;
    using fut_t = std::future<std::optional<int>>;
    std::deque<fut_t> pending_futures;
    MyLock reintegration_lock;
    std::vector<std::mutex> per_block_locks;
    std::fstream file;
    int fd;
    std::mutex dq_lock;
    std::mutex mapLock;
public:
    void create_file(const std::string filename) {
        fd = ::open(filename.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        ::ftruncate(fd, constants::FILE_SIZE);
        if (fd == -1) {
            LOG_ERR_MSG("unable to open fd for: ", filename);
        }
    }

    int write(const char* buf, int address, int size) {
//        LOG_DEBUG_MSG("writing @", address, ": ", std::string(buf, size));
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
//        LOG_DEBUG_MSG("reading @", address, ":", size, " -> ", std::string(buf, read_b));
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
        set_server_state(ServerState::PRIMARY);
        // set LOCKED state blocks to DISK state
        std::lock_guard lk(mapLock);
        for (auto& it: temp_data) {
            auto &info = it.second;
            if (info->state == BlockState::LOCKED) {
                info->state = BlockState::DISK;
            }
        }
        LOG_INFO_MSG(" -> PRIMARY");
    }
    void transition_to_backup() {
        transition(ServerState::PRIMARY);
        LOG_INFO_MSG(" -> BACKUP");
    }
    void writeToMap(const ds::WriteRequest *writeRequest, BlockState* state) {
//        mapLock.lock();
        LOG_DEBUG_MSG("temp map locked");
        {
            LOG_DEBUG_MSG("Writing to ", writeRequest->address());
            std::lock_guard lk(mapLock);
            temp_data[(int) writeRequest->address()] = std::make_shared<Info>(*state, writeRequest->data_length(),
                                                                              writeRequest->data());
        }
//        mapLock.unlock();
        LOG_DEBUG_MSG("temp map unlocked");
    }

    void updateMap(const int address, BlockState& state) {
//        mapLock.lock();
        LOG_DEBUG_MSG("temp map locked");
        {
            LOG_DEBUG_MSG("Updatinh to ",address);
//            std::lock_guard lk(mapLock);
            std::shared_ptr<Info> info = temp_data[address];
            info->state = state;
            temp_data[address] = info;
        }
//        mapLock.unlock();
        LOG_DEBUG_MSG("temp map unlocked");
    }
    explicit gRPCServiceImpl(
            std::shared_ptr<Channel> channel, const std::string filename) :
            stub_(gRPCService::NewStub(channel)), per_block_locks(constants::BLOCK_SIZE) {
        this->filename = filename;
        create_file(filename);
        LOG_DEBUG_MSG("Filename ", this->filename, " f:", filename);
    }

    void wait_before_read(const ds::ReadRequest* readRequest) {
        std::vector<int> blocks = get_blocks_involved(
                readRequest->address(), constants::BLOCK_SIZE);
        // change this to get signaled when the entry is removed
        // from the map (write to that block is complete)
        bool can_read_all = false;
//        if (readRequest->address() == 6) {
//            LOG_DEBUG_MSG("backup request to read a block");
//        }
        while(!can_read_all) {
            can_read_all = true;
            for (const int &b: blocks) {
                std::lock_guard lk(mapLock);
                for (int i = 0; i < 4096; i++) {
                    int block_addr = b * 4096 + i;
                    if (temp_data.count(block_addr) && temp_data[block_addr]->state == BlockState::LOCKED) {
                        can_read_all = false;
                        break;
                    }
                }
                if (!can_read_all)
                    break;
            }
            if (!can_read_all) {
                LOG_DEBUG_MSG("read waiting on locked: ", readRequest->address());
                std::this_thread::sleep_for(std::chrono::nanoseconds((int)1e5));
            }
        }
    }

    Status c_read(ServerContext *context, const ds::ReadRequest *readRequest,
        ds::ReadResponse *readResponse) {
        LOG_DEBUG_MSG("c_read function call");
        if (current_server_state_ == ServerState::BACKUP) {

            wait_before_read(readRequest);
            LOG_DEBUG_MSG("retuirned from wait");
        }
        if (get_server_state() == ServerState::BACKUP)
            LOG_DEBUG_MSG("reading from backup");
        else LOG_DEBUG_MSG("reading from primary");
        int buf_size = readRequest->data_length();
        auto buf = std::make_unique<char[]>(buf_size);
        ::bzero(buf.get(), buf_size);
        int bytes_read = read(buf.get(), readRequest->address(), buf_size);
//        LOG_DEBUG_MSG(std::string(buf.get(), bytes_read), " bytes read");
        readResponse->set_data(buf.get(), bytes_read);
        return Status::OK;
    }

    Status hb_check(ServerContext *context, const ds::HBRequest *request,ds::HBResponse *response) {

        return Status::OK;
    }

    Status hb_tell(ServerContext *context, const ds::HBRequest *request,ds::HBResponse *response) {

//        if (request->has_is_primary()) {
            if (request->is_primary()) {
                LOG_DEBUG_MSG("becoming primary");
                transition_to_primary();

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
//                reintegration_lock.unlock();
            }
//        }
        LOG_DEBUG_MSG("exiting from hb_tell");
        return Status::OK;
    }

    // Returns the block indices for the address and data_length.
    // In this case return vector size is at most 2
    std::vector<int> get_blocks_involved(const int address, const int data_length) {
        LOG_DEBUG_MSG(data_length);
        int first_block = address / constants::BLOCK_SIZE;
        int end_of_first_block = (first_block + 1) * constants::BLOCK_SIZE - 1;
        int first_block_size_left = end_of_first_block - address + 1;
        LOG_DEBUG_MSG("first block num", first_block);
        std::vector<int> blocks_involved;
        blocks_involved.push_back(first_block);
        if (data_length > first_block_size_left) {
            blocks_involved.push_back(first_block + 1);
            LOG_DEBUG_MSG("second block added ", first_block+1);
        }
        return blocks_involved;
    }

    template<class T>
    void get_write_locks(const T *writeRequest) {
        auto blocks = get_blocks_involved(
                writeRequest->address(), writeRequest->data_length());
        LOG_DEBUG_MSG("Trying to get re_int lock");
        if (current_server_state_ == ServerState::PRIMARY) {
            reintegration_lock.lock_shared();
        }
        LOG_DEBUG_MSG("TOOK SHARED LOCK");

        for (const int &block: blocks) {
            LOG_DEBUG_MSG("trying to get lock on ", block);
            per_block_locks.at(block).lock();
        }
        for (auto block_num : blocks) {
            LOG_DEBUG_MSG("locks acquired on ", block_num);
        }
    }

    template<class T>
    void release_write_locks(const T *writeRequest) {
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
        std::lock_guard lk(mapLock);
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
        std::lock_guard lk(mapLock);
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
        {
            std::lock_guard lk(mapLock);
            temp_data.clear();
        }
        LOG_DEBUG_MSG("Size of temp_data", temp_data.size());
        reintegration_lock.unlock();
        LOG_DEBUG_MSG("RE-int lock released");
        return Status::OK;
    }

    void secondary_reintegration() {
        LOG_DEBUG_MSG("here");
        long double reintegration_time_start = time_monotonic();

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
            LOG_ERR_MSG("error: ", status.error_code(), status.error_message());
        }
        const static bool SERVER_CRASH_AFTER_REINTEGRATION_PHASE_1 = std::getenv("SERVER_CRASH_AFTER_REINTEGRATION_PHASE_1");
        if (SERVER_CRASH_AFTER_REINTEGRATION_PHASE_1) {
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
        int disk_records_written = reintegration_response.data_size();
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
            LOG_ERR_MSG("error: ", status.error_code(), status.error_message());
        }

        const static bool SERVER_CRASH_AFTER_REINTEGRATION_PHASE_2 = std::getenv("SERVER_CRASH_AFTER_REINTEGRATION_PHASE_2");
        if (SERVER_CRASH_AFTER_REINTEGRATION_PHASE_2) {
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
            LOG_ERR_MSG("error: ", status.error_code(), status.error_message());
        }

        //set_backup_state(BackupState::ALIVE);
        LOG_DEBUG_MSG("reintegration complete");
        LOG_DEBUG_MSG(std::fixed, std::setprecision(2), "reint, ", time_monotonic() - reintegration_time_start, ", records_updated,",
                     disk_records_written + reintegration_response.data_size());

    }

    Status c_write(ServerContext *context, const ds::WriteRequest *writeRequest,
                   ds::WriteResponse *writeResponse) {
        LOG_DEBUG_MSG("WRITE request");
        if (writeRequest->address() > constants::FILE_SIZE ||
                (writeRequest->address() + writeRequest->data_length()) > constants::FILE_SIZE) {
            LOG_ERR_MSG("Write request adddress ", writeRequest->address() + writeRequest->data_length(), " out of bounds");
            return Status::CANCELLED;
        }
//        assert_msg(current_server_state_ != ServerState::PRIMARY,
//                   "Reintegration called on backup");
        if (current_server_state_ != ServerState::PRIMARY) {
            return Status::CANCELLED;
        }
        const BackupState current_backup_state = backup_state;
        LOG_DEBUG_MSG("Starting primary server write");
//        {
//            decltype(pending_futures) tdq;
//            {
//                if (pending_futures.size()) {
//                    std::lock_guard l(dq_lock); // TODO: make unique_lock
//                    if (pending_futures.size() > 0)
//                        tdq.swap(pending_futures);
//                }
//            }
//            while (tdq.size()) {
//                LOG_DEBUG_MSG(tdq.size(), " pending futures found");
//                auto pf = std::move(tdq.front());
//                tdq.pop_front();
////                l.unlock();
//                if (pf.valid()) {
//                    LOG_DEBUG_MSG("future ready, decreasing size of map");
//                    const auto addr = pf.get();
//                    if (addr) {
//                        LOG_DEBUG_MSG("address found in optional int", *addr);
////                        std::lock_guard lk(mapLock);
////                        temp_data.erase(addr.value());
//                    }
//                    LOG_DEBUG_MSG("temp_data size:", temp_data.size());
//                } else {
//                    std::lock_guard l(dq_lock);
//                    pending_futures.push_back(std::move(pf));
//                }
//            }
//        }
        get_write_locks(writeRequest);
        BlockState state = (current_backup_state == BackupState::REINTEGRATION) ? BlockState::MEMORY : BlockState::DISK;
        LOG_DEBUG_MSG("Backup state:", current_backup_state == BackupState::REINTEGRATION ? "r" : "not r");
        LOG_DEBUG_MSG("wrtiing in primary with state ", ((state == BlockState::DISK) ? "DISK" : "MEM"));
        writeToMap(writeRequest, &state);

        bool write_sent_to_backup = false;
        LOG_DEBUG_MSG("temp_data size:" + std::to_string(temp_data.size()));
        if (current_backup_state == BackupState::ALIVE) {
            ClientContext context;
            ds::AckResponse ackResponse;
            LOG_DEBUG_MSG("sending write to backup");
            Status status = stub_->s_write(&context, *writeRequest, &ackResponse);
            if (!status.ok()) {
                LOG_ERR_MSG("error ", status.error_code(), status.error_message());
            } else {
                write_sent_to_backup = true;
            }

            const static bool SERVER_CRASH_AFTER_BACKUP_WRITE = std::getenv("SERVER_CRASH_AFTER_BACKUP_WRITE");
            if (SERVER_CRASH_AFTER_BACKUP_WRITE) {
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

        if (current_backup_state == BackupState::ALIVE && write_sent_to_backup) {
            LOG_DEBUG_MSG("commit to backup");
            ClientContext context;
            ds::CommitRequest commitRequest;
            const int waddr = writeRequest->address();
            const int len = writeRequest->data_length();
            LOG_DEBUG_MSG("waddr to ret = ", waddr);
            fut_t f = std::async(std::launch::async,
                [&, waddr]() -> std::optional<int> {
                    ClientContext context;
                    ds::CommitRequest commitRequest;
                    commitRequest.set_address(waddr);
                    commitRequest.set_data_length(len);
                    ds::AckResponse ackResponse;
                    if ((stub_->s_commit(&context, commitRequest, &ackResponse)).ok()) {
                        LOG_DEBUG_MSG("waddr to ret = ", waddr);
                        return waddr;
                    }
                    return std::nullopt;
            });
            {
                std::lock_guard l(dq_lock);
                pending_futures.push_back(std::move(f));
            }
            const static bool SERVER_CRASH_AFTER_SENDING_BACKUP_COMMIT = std::getenv("SERVER_CRASH_AFTER_SENDING_BACKUP_COMMIT");
            if (SERVER_CRASH_AFTER_SENDING_BACKUP_COMMIT) {
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
        if (current_server_state_ != ServerState::BACKUP) {
            LOG_ERR_MSG("calling s_write at primary whyyy?");
            return Status::CANCELLED;
        }
        LOG_DEBUG_MSG("Starting backup server write");
        BlockState state = BlockState::LOCKED;
        writeToMap(writeRequest, &state);
        LOG_DEBUG_MSG("Pausing write in backup server");
        LOG_DEBUG_MSG("waking from sloeep");
        return Status::OK;
    }

    Status s_commit(ServerContext *context, const ds::CommitRequest *commitRequest,
        ds::AckResponse *ackResponse) {
//        assert_msg(current_server_state_ != ServerState::BACKUP,
//                   "Reintegration called on backup");
        LOG_DEBUG_MSG("calling commit on backup");
        std::lock_guard lk(mapLock);

        LOG_DEBUG_MSG("Committing to ", commitRequest->address());
        const auto it = temp_data.find(commitRequest->address());
        if (it == temp_data.end()) {
            LOG_ERR_MSG("it==tmp_data.end()");
        }
        LOG_ERR_MSG("it==tmp_data.end() 1");
        auto& info = it->second;
        LOG_ERR_MSG("it==tmp_data.end() 2");
        get_write_locks(commitRequest);
        write(info->data.c_str(), commitRequest->address(), info->length);
        release_write_locks(commitRequest);
        BlockState state = BlockState::DISK;
//        writeToMap(commitRequest, &state);
        std::shared_ptr<Info> info1 = temp_data[commitRequest->address()];
        info1->state = state;
        temp_data[commitRequest->address()] = info1;
//        updateMap(commitRequest->address(), state);
//        temp_data.erase(it);
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