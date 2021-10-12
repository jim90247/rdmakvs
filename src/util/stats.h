#pragma once
#include <chrono>
#include <cmath>

/**
 * @brief Computes the "operations per second" statistic.
 *
 * @param start the start time of measurement
 * @param end the end time of measurement
 * @param operations the total operations performed from {@code start} to {@code end}
 * @return the operation per second statistic
 */
inline double ComputeOperationsPerSecond(std::chrono::time_point<std::chrono::steady_clock> start,
                                         std::chrono::time_point<std::chrono::steady_clock> end,
                                         long operations) {
    if (start == end) {
        return std::nan("ComputeOperationsPerSecond");
    }
    return operations / std::chrono::duration<double>(end - start).count();
}
