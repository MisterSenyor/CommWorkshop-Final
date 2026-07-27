#include "process_group_api.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>

int main(int argc, char** argv) {
    std::string servername = "";
    int my_index = -1;
    std::vector<std::string> host_list;

    // Parse CLI arguments matching assignment format: -myindex <idx> -list <node1> <node2> ...
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "-myindex") == 0 && i + 1 < argc) {
            my_index = std::stoi(argv[++i]);
        } else if (std::strcmp(argv[i], "-list") == 0) {
            while (i + 1 < argc && argv[i + 1][0] != '-') {
                host_list.push_back(argv[++i]);
            }
        }
    }

    if (my_index < 0 || host_list.empty()) {
        std::cerr << "Usage: " << argv[0] << " -myindex <index> -list <host1> <host2> ..." << std::endl;
        return 1;
    }

    servername = host_list[0]; // Master server host for initial handshake

    std::cout << "[Rank " << my_index << "] Connecting process group via server: " << servername << std::endl;

    // 1. Connect process group
    void* pg_handle = nullptr;
    if (connect_process_group(const_cast<char*>(servername.c_str()), &pg_handle) != 0) {
        std::cerr << "[Rank " << my_index << "] Failed to connect process group." << std::endl;
        return 1;
    }

    // 2. Prepare test data
    const int count = 1000;
    std::vector<float> sendbuf(count, 1.0f); // Every process starts with 1.0
    std::vector<float> recvbuf(count, 0.0f);

    // 3. Execute All-Reduce
    std::cout << "[Rank " << my_index << "] Running pg_all_reduce..." << std::endl;
    int status = pg_all_reduce(sendbuf.data(), recvbuf.data(), count, PG_FLOAT, PG_SUM, pg_handle);

    if (status == 0) {
        std::cout << "[Rank " << my_index << "] All-Reduce completed successfully!" << std::endl;
        std::cout << "[Rank " << my_index << "] Sample Output[0]: " << recvbuf[0] << " (Expected: " << host_list.size() << ".0)" << std::endl;
    } else {
        std::cerr << "[Rank " << my_index << "] All-Reduce failed with error code: " << status << std::endl;
    }

    // 4. Destroy process group handles
    pg_close(pg_handle);
    return 0;
}