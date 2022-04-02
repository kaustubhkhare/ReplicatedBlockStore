#include <iostream>
#include <stdexcept>

//fwd declare all tests
#define REG_TEST(x) void test##x(int, char**)
#include "test_list.inc"
#undef REG_TEST


int main(int argc, char** argv)
{
    if (argc < 2) {
        std::runtime_error("need test name to run");
    }
    const std::string tname(argv[1]);
    argc--;
    argv++;
#define IF_TEST(x) if (tname == #x) test##x(argc, argv)
#define REG_TEST(x) IF_TEST(x)
#include "test_list.inc"
#undef REG_TEST
#undef IF_TEST
}
