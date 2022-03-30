#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <memory>
#include "iostream"
#include "helper.h"
#include "constants.h"

int main(int argc, char* argv[]) {
    std::string filename = "/users/kkhare/DS/ReplicatedBlockStore/build/test_data.txt";
    int fd = open(filename.c_str(), O_RDWR|O_CREAT, S_IRWXU);
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
    int data_size = 8;
    int address = 0;
//    auto buf = std::make_unique<std::string>(data_size, 'a');
    std::string buf(data_size, 'a');
    LOG_DEBUG_MSG("Writing ", buf);

    int bytes = pwrite(fd, buf.c_str(), data_size, address);
    LOG_DEBUG_MSG(bytes, " bytes written");

    auto bufN = std::make_unique<char[]>(data_size);
    int  read_bytes = pread(fd, bufN.get(), data_size, address);
    LOG_DEBUG_MSG("Reading ", bufN.get());

//    std::size_t hin = std::hash<std::string>{}(buf);
//    std::size_t hout = std::hash<std::string>{}(bufRead);
//    if (hin != hout) {
//        LOG_DEBUG_MSG("not equal");
//    } else {
//        LOG_DEBUG_MSG("equal");
//    }
    close(fd);
}
