#ifndef PROJECT3_CONSTANTS_H
#define PROJECT3_CONSTANTS_H

namespace constants {
    inline constexpr int FILE_SIZE = 2 * (1024 * 1024 * 1024);
    inline constexpr int BLOCK_SIZE {4096};
    inline constexpr int TOTAL_BLOCKS = FILE_SIZE / BLOCK_SIZE;
    inline constexpr int PRIMARY_PORT {8083};
    inline constexpr int BACKUP_PORT {8084};
    inline constexpr int MAX_RECONN_TIMEOUT {500};
}

#endif //PROJECT3_CONSTANTS_H
