#include "process_group_api.h"

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

namespace {

struct Options {
    std::string raw_index;
    bool zero_based = false;
    int repetitions = 1;
    std::vector<std::string> hosts;
};

void usage(const char* program) {
    std::cerr
        << "Usage: " << program
        << " -myindex <01|02|...> -list <host1> <host2> ... "
           "[-repetitions N] [--zero-based]\n";
}

bool parse_options(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "-myindex") == 0 && i + 1 < argc) {
            options.raw_index = argv[++i];
        } else if (std::strcmp(argv[i], "-list") == 0) {
            while (i + 1 < argc && argv[i + 1][0] != '-') {
                options.hosts.emplace_back(argv[++i]);
            }
        } else if (std::strcmp(argv[i], "-repetitions") == 0 && i + 1 < argc) {
            options.repetitions = std::stoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--zero-based") == 0) {
            options.zero_based = true;
        } else {
            return false;
        }
    }
    return !options.raw_index.empty() && !options.hosts.empty() &&
           options.repetitions > 0;
}

int resolve_rank(const Options& options) {
    const int supplied = std::stoi(options.raw_index);
    if (options.zero_based) {
        return supplied;
    }
    // Match the assignment examples: 01, 02, 03, 04 map to ranks 0,1,2,3.
    if (supplied >= 1 && supplied <= static_cast<int>(options.hosts.size())) {
        return supplied - 1;
    }
    // Also permit an explicit zero for rank zero.
    return supplied;
}

std::string join_hosts(const std::vector<std::string>& hosts) {
    std::string result;
    for (std::size_t i = 0; i < hosts.size(); ++i) {
        if (i != 0) result += ',';
        result += hosts[i];
    }
    return result;
}

template <typename T, typename MakeValue, typename MakeExpected>
bool run_case(void* handle,
              int rank,
              const std::string& name,
              int count,
              DATATYPE datatype,
              OPERATION operation,
              bool in_place,
              MakeValue make_value,
              MakeExpected make_expected,
              double tolerance) {
    std::vector<T> send(static_cast<std::size_t>(count));
    std::vector<T> receive(static_cast<std::size_t>(count), T{});
    for (int i = 0; i < count; ++i) {
        send[static_cast<std::size_t>(i)] = make_value(rank, i);
    }

    void* send_pointer = send.data();
    void* receive_pointer = receive.data();
    if (in_place) {
        receive = send;
        send_pointer = receive.data();
    }

    const int status = pg_all_reduce(send_pointer,
                                     receive_pointer,
                                     count,
                                     datatype,
                                     operation,
                                     handle);
    if (status != PG_SUCCESS) {
        std::cerr << "[Rank " << rank << "] CASE " << name
                  << " FAIL: pg_all_reduce status=" << status << '\n';
        return false;
    }

    for (int i = 0; i < count; ++i) {
        const double actual = static_cast<double>(receive[static_cast<std::size_t>(i)]);
        const double expected = static_cast<double>(make_expected(i));
        if (std::fabs(actual - expected) > tolerance) {
            std::cerr << "[Rank " << rank << "] CASE " << name
                      << " FAIL at index " << i << ": actual=" << actual
                      << " expected=" << expected << '\n';
            return false;
        }
    }

    std::cout << "[Rank " << rank << "] CASE " << name << " PASS"
              << " count=" << count << " first=" << receive.front()
              << " last=" << receive.back() << '\n';
    return true;
}

} // namespace

int main(int argc, char** argv) {
    Options options;
    try {
        if (!parse_options(argc, argv, options)) {
            usage(argv[0]);
            return 2;
        }
    } catch (const std::exception& error) {
        std::cerr << "Argument error: " << error.what() << '\n';
        usage(argv[0]);
        return 2;
    }

    const int rank = resolve_rank(options);
    const int world_size = static_cast<int>(options.hosts.size());
    if (rank < 0 || rank >= world_size) {
        std::cerr << "Resolved rank " << rank << " is outside [0,"
                  << world_size << ").\n";
        return 2;
    }

    const std::string rank_string = std::to_string(rank);
    const std::string size_string = std::to_string(world_size);
    const std::string hosts_csv = join_hosts(options.hosts);
    setenv("RANK", rank_string.c_str(), 1);
    setenv("WORLD_SIZE", size_string.c_str(), 1);
    setenv("HOST_LIST", hosts_csv.c_str(), 1);

    std::cout << "[Rank " << rank << "] host=" << options.hosts[rank]
              << " world_size=" << world_size
              << " bootstrap=" << options.hosts.front() << '\n';

    void* handle = nullptr;
    std::string bootstrap = options.hosts.front();
    const int connect_status =
        connect_process_group(bootstrap.data(), &handle);
    if (connect_status != PG_SUCCESS) {
        std::cerr << "[Rank " << rank
                  << "] CONNECT FAIL status=" << connect_status << '\n';
        return 1;
    }

    bool all_passed = true;
    const int sum_ranks = world_size * (world_size + 1) / 2;

    for (int repetition = 0; repetition < options.repetitions; ++repetition) {
        all_passed &= run_case<std::int32_t>(
            handle,
            rank,
            "int32-sum-uneven",
            1003,
            PG_INT32,
            PG_SUM,
            false,
            [](int r, int i) { return static_cast<std::int32_t>((r + 1) * 100 + i); },
            [world_size, sum_ranks](int i) {
                return static_cast<std::int32_t>(100 * sum_ranks + world_size * i);
            },
            0.0);

        all_passed &= run_case<float>(
            handle,
            rank,
            "float32-sum",
            257,
            PG_FLOAT32,
            PG_SUM,
            false,
            [](int r, int i) { return 0.5F * static_cast<float>(r + 1) + static_cast<float>(i); },
            [world_size, sum_ranks](int i) {
                return 0.5F * static_cast<float>(sum_ranks) +
                       static_cast<float>(world_size * i);
            },
            1e-3);

        all_passed &= run_case<double>(
            handle,
            rank,
            "float64-max",
            65,
            PG_FLOAT64,
            PG_MAX,
            false,
            [](int r, int i) { return 1000.0 * r + i; },
            [world_size](int i) { return 1000.0 * (world_size - 1) + i; },
            1e-12);

        all_passed &= run_case<std::int32_t>(
            handle,
            rank,
            "int32-sum-in-place",
            129,
            PG_INT32,
            PG_SUM,
            true,
            [](int r, int i) { return static_cast<std::int32_t>(r + i); },
            [world_size](int i) {
                return static_cast<std::int32_t>(
                    world_size * i + world_size * (world_size - 1) / 2);
            },
            0.0);
    }

    const int close_status = pg_close(handle);
    if (close_status != PG_SUCCESS) {
        std::cerr << "[Rank " << rank << "] CLOSE FAIL status="
                  << close_status << '\n';
        all_passed = false;
    }

    std::cout << "[Rank " << rank << "] TEST_RESULT "
              << (all_passed ? "PASS" : "FAIL") << '\n';
    return all_passed ? 0 : 1;
}
