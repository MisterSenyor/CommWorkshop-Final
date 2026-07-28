#include "process_group_api.h"

#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>

int main(int argc, char** argv) {
    int my_index = -1;
    std::vector<std::string> host_list;

    // Parse command line arguments: -myindex <index> -list <host1> <host2> ...
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

    // Export environment variables for the C API layer
    std::string rank_str = std::to_string(my_index);
    std::string size_str = std::to_string(host_list.size());
    setenv("RANK", rank_str.c_str(), 1);
    setenv("WORLD_SIZE", size_str.c_str(), 1);

    std::string hosts_csv;
    for (size_t i = 0; i < host_list.size(); ++i) {
        hosts_csv += host_list[i] + (i + 1 < host_list.size() ? "," : "");
    }
    setenv("HOST_LIST", hosts_csv.c_str(), 1);

    std::string servername = host_list[0];

    std::cout << "[Rank " << my_index << "] Connecting process group (" 
              << host_list.size() << " ranks) via server: " << servername << std::endl;

    void* pg_handle = nullptr;
    if (connect_process_group(const_cast<char*>(servername.c_str()), &pg_handle) != 0) {
        std::cerr << "[Rank " << my_index << "] Failed to connect process group." << std::endl;
        return 1;
    }

    const int count = 1000;
    std::vector<float> sendbuf(count, 1.0f);
    std::vector<float> recvbuf(count, 0.0f);

    std::cout << "[Rank " << my_index << "] Running pg_all_reduce..." << std::endl;
    int status = pg_all_reduce(sendbuf.data(), recvbuf.data(), count, PG_FLOAT32, PG_SUM, pg_handle);

    if (status == 0) {
        std::cout << "[Rank " << my_index << "] All-Reduce completed successfully!" << std::endl;
        std::cout << "[Rank " << my_index << "] Output[0]: " << recvbuf[0] 
                  << " (Expected: " << host_list.size() << ".0)" << std::endl;
    } else {
        std::cerr << "[Rank " << my_index << "] All-Reduce failed with status: " << status << std::endl;
    }

    pg_close(pg_handle);
    return 0;
}