#include "process_group_api.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>

namespace {

struct Options {
    std::string raw_index;
    std::vector<std::string> hosts;
    std::string mode;
    std::string sizes =
        "64,256,1024,2048,4096,8192,16384,65536,262144,1048576,4194304,16777216";
    int warmup = 10;
    int repetitions = 0; // 0 means adaptive by payload size.
    std::size_t piece_bytes = 64U * 1024U;
    unsigned int max_inflight = 4U;
    int trial = 1;
    bool zero_based = false;
};

void usage(const char* program) {
    std::cerr
        << "Usage: " << program
        << " -myindex <01|02|...> -list <host1> <host2> ..."
           " -mode <eager|rendezvous>"
           " [-sizes comma-separated-bytes] [-warmup N]"
           " [-repetitions N] [-piece-bytes N] [-max-inflight N]"
           " [-trial N] [--zero-based]\n";
}

std::size_t parse_size(const std::string& text) {
    if (text.empty()) {
        throw std::invalid_argument("empty size");
    }

    std::size_t multiplier = 1U;
    std::string number = text;
    const char suffix = text.back();
    if (suffix == 'K' || suffix == 'k') {
        multiplier = 1024U;
        number.pop_back();
    } else if (suffix == 'M' || suffix == 'm') {
        multiplier = 1024U * 1024U;
        number.pop_back();
    } else if (suffix == 'G' || suffix == 'g') {
        multiplier = 1024U * 1024U * 1024U;
        number.pop_back();
    }

    std::size_t consumed = 0U;
    const unsigned long long parsed = std::stoull(number, &consumed, 10);
    if (consumed != number.size() || parsed == 0ULL) {
        throw std::invalid_argument("invalid size: " + text);
    }
    return static_cast<std::size_t>(parsed) * multiplier;
}

std::vector<std::size_t> parse_sizes(const std::string& text) {
    std::vector<std::size_t> result;
    std::size_t start = 0U;
    while (start <= text.size()) {
        const std::size_t comma = text.find(',', start);
        const std::string token = text.substr(
            start, comma == std::string::npos ? std::string::npos : comma - start);
        const std::size_t bytes = parse_size(token);
        if ((bytes % sizeof(std::int32_t)) != 0U) {
            throw std::invalid_argument(
                "every size must be divisible by sizeof(int32_t): " + token);
        }
        result.push_back(bytes);
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1U;
    }
    if (result.empty()) {
        throw std::invalid_argument("size list is empty");
    }
    return result;
}

bool parse_options(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "-myindex") == 0 && i + 1 < argc) {
            options.raw_index = argv[++i];
        } else if (std::strcmp(argv[i], "-list") == 0) {
            while (i + 1 < argc && argv[i + 1][0] != '-') {
                options.hosts.emplace_back(argv[++i]);
            }
        } else if (std::strcmp(argv[i], "-mode") == 0 && i + 1 < argc) {
            options.mode = argv[++i];
        } else if (std::strcmp(argv[i], "-sizes") == 0 && i + 1 < argc) {
            options.sizes = argv[++i];
        } else if (std::strcmp(argv[i], "-warmup") == 0 && i + 1 < argc) {
            options.warmup = std::stoi(argv[++i]);
        } else if (std::strcmp(argv[i], "-repetitions") == 0 && i + 1 < argc) {
            options.repetitions = std::stoi(argv[++i]);
        } else if (std::strcmp(argv[i], "-piece-bytes") == 0 && i + 1 < argc) {
            options.piece_bytes = parse_size(argv[++i]);
        } else if (std::strcmp(argv[i], "-max-inflight") == 0 && i + 1 < argc) {
            options.max_inflight =
                static_cast<unsigned int>(std::stoul(argv[++i]));
        } else if (std::strcmp(argv[i], "-trial") == 0 && i + 1 < argc) {
            options.trial = std::stoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--zero-based") == 0) {
            options.zero_based = true;
        } else {
            return false;
        }
    }

    return !options.raw_index.empty() && !options.hosts.empty() &&
           (options.mode == "eager" || options.mode == "rendezvous") &&
           options.warmup >= 0 && options.repetitions >= 0 &&
           options.piece_bytes > 0U && options.max_inflight > 0U &&
           options.trial > 0;
}

int resolve_rank(const Options& options) {
    const int supplied = std::stoi(options.raw_index);
    if (options.zero_based) {
        return supplied;
    }
    if (supplied >= 1 && supplied <= static_cast<int>(options.hosts.size())) {
        return supplied - 1;
    }
    return supplied;
}

std::string join_hosts(const std::vector<std::string>& hosts) {
    std::string result;
    for (std::size_t i = 0; i < hosts.size(); ++i) {
        if (i != 0U) {
            result += ',';
        }
        result += hosts[i];
    }
    return result;
}

std::string local_hostname() {
    char buffer[256]{};
    if (::gethostname(buffer, sizeof(buffer) - 1U) != 0) {
        return "unknown";
    }
    return buffer;
}

int adaptive_repetitions(std::size_t bytes) {
    if (bytes <= 4U * 1024U) {
        return 300;
    }
    if (bytes <= 64U * 1024U) {
        return 150;
    }
    if (bytes <= 1024U * 1024U) {
        return 60;
    }
    if (bytes <= 4U * 1024U * 1024U) {
        return 25;
    }
    return 10;
}

std::int32_t input_value(int rank, int index) {
    return static_cast<std::int32_t>(17 * (rank + 1) + (index % 251));
}

std::int32_t expected_value(int world_size, int index) {
    const int rank_sum = world_size * (world_size + 1) / 2;
    return static_cast<std::int32_t>(17 * rank_sum +
                                     world_size * (index % 251));
}

bool verify_result(const std::vector<std::int32_t>& output,
                   int world_size,
                   int rank,
                   std::size_t bytes,
                   const std::string& stage) {
    for (std::size_t i = 0; i < output.size(); ++i) {
        const std::int32_t expected =
            expected_value(world_size, static_cast<int>(i));
        if (output[i] != expected) {
            std::cerr << "[Rank " << rank << "] correctness failure "
                      << "stage=" << stage << " bytes=" << bytes
                      << " index=" << i << " actual=" << output[i]
                      << " expected=" << expected << '\n';
            return false;
        }
    }
    return true;
}

int run_collective(void* handle,
                   std::vector<std::int32_t>& input,
                   std::vector<std::int32_t>& output) {
    return pg_all_reduce(input.data(),
                         output.data(),
                         static_cast<int>(input.size()),
                         PG_INT32,
                         PG_SUM,
                         handle);
}

double percentile(std::vector<double> values, double probability) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double position = probability * static_cast<double>(values.size() - 1U);
    const std::size_t lower = static_cast<std::size_t>(std::floor(position));
    const std::size_t upper = static_cast<std::size_t>(std::ceil(position));
    const double fraction = position - static_cast<double>(lower);
    return values[lower] * (1.0 - fraction) + values[upper] * fraction;
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

    std::vector<std::size_t> sizes;
    try {
        sizes = parse_sizes(options.sizes);
    } catch (const std::exception& error) {
        std::cerr << "Invalid size list: " << error.what() << '\n';
        return 2;
    }

    const std::string rank_string = std::to_string(rank);
    const std::string size_string = std::to_string(world_size);
    const std::string hosts_csv = join_hosts(options.hosts);
    const std::string piece_string = std::to_string(options.piece_bytes);
    const std::string inflight_string = std::to_string(options.max_inflight);

    setenv("RANK", rank_string.c_str(), 1);
    setenv("WORLD_SIZE", size_string.c_str(), 1);
    setenv("HOST_LIST", hosts_csv.c_str(), 1);
    setenv("PG_TRANSFER_MODE", options.mode.c_str(), 1);
    setenv("PG_PIPELINE_PIECE_BYTES", piece_string.c_str(), 1);
    setenv("PG_MAX_INFLIGHT", inflight_string.c_str(), 1);

    std::cout << "[Rank " << rank << "] benchmark mode=" << options.mode
              << " trial=" << options.trial
              << " world_size=" << world_size
              << " piece_bytes=" << options.piece_bytes
              << " max_inflight=" << options.max_inflight << '\n';

    void* handle = nullptr;
    std::string bootstrap = options.hosts.front();
    const int connect_status = connect_process_group(bootstrap.data(), &handle);
    if (connect_status != PG_SUCCESS) {
        std::cerr << "[Rank " << rank
                  << "] CONNECT FAIL status=" << connect_status << '\n';
        return 1;
    }

    bool passed = true;
    const std::string hostname = local_hostname();

    for (const std::size_t total_bytes : sizes) {
        const std::size_t count = total_bytes / sizeof(std::int32_t);
        if (count > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
            std::cerr << "Payload too large for API count: " << total_bytes << '\n';
            passed = false;
            break;
        }

        const int repetitions = options.repetitions > 0
                                    ? options.repetitions
                                    : adaptive_repetitions(total_bytes);

        std::vector<std::int32_t> input(count);
        std::vector<std::int32_t> output(count, 0);
        for (std::size_t i = 0; i < count; ++i) {
            input[i] = input_value(rank, static_cast<int>(i));
        }

        if (run_collective(handle, input, output) != PG_SUCCESS ||
            !verify_result(output, world_size, rank, total_bytes, "precheck")) {
            passed = false;
            break;
        }

        for (int i = 0; i < options.warmup; ++i) {
            if (run_collective(handle, input, output) != PG_SUCCESS) {
                std::cerr << "[Rank " << rank << "] warmup failure bytes="
                          << total_bytes << " iteration=" << i << '\n';
                passed = false;
                break;
            }
        }
        if (!passed) {
            break;
        }

        std::vector<double> latencies_us;
        latencies_us.reserve(static_cast<std::size_t>(repetitions));
        for (int iteration = 0; iteration < repetitions; ++iteration) {
            const auto start = std::chrono::steady_clock::now();
            const int status = run_collective(handle, input, output);
            const auto end = std::chrono::steady_clock::now();
            if (status != PG_SUCCESS) {
                std::cerr << "[Rank " << rank << "] measured failure bytes="
                          << total_bytes << " iteration=" << iteration
                          << " status=" << status << '\n';
                passed = false;
                break;
            }
            const double microseconds =
                std::chrono::duration<double, std::micro>(end - start).count();
            latencies_us.push_back(microseconds);
        }
        if (!passed) {
            break;
        }

        if (!verify_result(output, world_size, rank, total_bytes, "postcheck")) {
            passed = false;
            break;
        }

        const double mean = std::accumulate(latencies_us.begin(),
                                            latencies_us.end(),
                                            0.0) /
                            static_cast<double>(latencies_us.size());
        std::cout << std::fixed << std::setprecision(3)
                  << "[Rank " << rank << "] SIZE_RESULT mode=" << options.mode
                  << " bytes=" << total_bytes
                  << " repetitions=" << repetitions
                  << " mean_us=" << mean
                  << " median_us=" << percentile(latencies_us, 0.50)
                  << " p95_us=" << percentile(latencies_us, 0.95)
                  << " PASS\n";

        for (std::size_t iteration = 0; iteration < latencies_us.size(); ++iteration) {
            std::cout << std::fixed << std::setprecision(3)
                      << "CSV," << options.trial
                      << ',' << world_size
                      << ',' << rank
                      << ',' << hostname
                      << ',' << options.mode
                      << ',' << total_bytes
                      << ',' << count
                      << ',' << options.piece_bytes
                      << ',' << options.max_inflight
                      << ',' << options.warmup
                      << ',' << repetitions
                      << ',' << iteration
                      << ',' << latencies_us[iteration]
                      << '\n';
        }
    }

    const int close_status = pg_close(handle);
    if (close_status != PG_SUCCESS) {
        std::cerr << "[Rank " << rank << "] CLOSE FAIL status="
                  << close_status << '\n';
        passed = false;
    }

    std::cout << "[Rank " << rank << "] BENCHMARK_RESULT "
              << (passed ? "PASS" : "FAIL") << '\n';
    return passed ? 0 : 1;
}
