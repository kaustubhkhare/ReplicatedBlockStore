#include "client.h"

int main(int argc, char *argv[]) {
//    if (argc < 5) {
//        printf("Usage : ./client -ip <ip> -port <port>\n");
//        return 0;
//    }
    auto client = GRPCClient::get_client(argc, argv);

    while (true) {
        std::string command;
        int offset;
        int length;
        std::cout << "Enter command (r/w/x)";
        std::cin >> command;
        if (command == "r") {
            std::cout << "Enter offset:";
            std::cin >> offset;
            std::cout << "Enter length:";
            std::cin >> length;
            std::string read_str = client.read(offset, length);
            std::cout << read_str << "\n";
        } else if (command == "w") {
            std::string v;
            std::cout << "Enter offset:";
            std::cin >> offset;
            std::cout << "Enter length:";
            std::cin >> length;
            std::cout << "Enter string:";
            std::cin >> v;
            while (v.length() != length) {
                std::cout << "Length of string should be " << length << "\n";
                std::cout << "Enter string:";
                std::cin >> v;
            }
            client.write(offset, length, v.c_str());
        } else {
            break;
        }
    }
//    std::string buf(10, 'b');
//    int v = client.write(1000, buf.length(), buf.c_str());
//    std::string bufRead = client.read(1000, 10);
////    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    std::cout << "Writing" << buf << "\n";
////    bufRead = client.read();
////    std::cout << bufRead[0] << bufRead[1]<<"\n";
//    std::cout << "Reading" << bufRead << "\n";
    return 0;
}
