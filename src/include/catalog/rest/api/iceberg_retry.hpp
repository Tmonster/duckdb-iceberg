#pragma once

#include "duckdb/common/common.hpp"

#include <chrono>
#include <random>
#include <thread>

namespace duckdb {

struct IcebergTableMetadata;

//! Resolved `commit.retry.*` table properties controlling the optimistic-concurrency retry loop.
//! Defaults mirror Apache Iceberg Java (the authoritative reference).
struct IcebergRetryConfig {
	idx_t num_retries = 4;                  // commit.retry.num-retries (default 4 -> 5 attempts total)
	int64_t min_wait_ms = 100;              // commit.retry.min-wait-ms
	int64_t max_wait_ms = 60 * 1000;        // commit.retry.max-wait-ms
	int64_t total_wait_ms = 30 * 60 * 1000; // commit.retry.total-timeout-ms

	static IcebergRetryConfig FromTableMetadata(const IcebergTableMetadata &metadata);

	//! Combine two configs into the most lenient (most retry-tolerant) of the two, taking the
	//! larger num-retries / max-wait / total-timeout and the smaller min-wait. Used when a single
	//! transaction commits multiple tables atomically: the retry loop is shared, so no table's
	//! (more permissive) policy should be silently dropped in favor of an arbitrary "first" table.
	IcebergRetryConfig MostLenient(const IcebergRetryConfig &other) const;

	//! Decorrelated jitter (AWS "Exponential Backoff And Jitter"): the next sleep is drawn from
	//! [min_wait, prev_sleep*3], clamped to max_wait. Starting `prev_sleep` is min_wait, so the first
	//! retries are TIGHT (near min_wait) -- a conflict's refresh+recommit window is short, so retrying
	//! quickly usually wins -- and the window only widens as repeated failures accumulate, which is
	//! exactly when a thundering herd needs spreading. `unit_random` in [0,1). Returns the new sleep;
	//! the caller feeds it back as `prev_sleep` next iteration. Pure/unit-testable.
	int64_t DecorrelatedBackoffMs(int64_t prev_sleep_ms, double unit_random) const;
};

//! Per-retry-loop backoff state: decorrelated jitter (de-synchronizes a thundering herd of
//! concurrent writers) plus a cumulative total-timeout budget. Independent RNG per loop so
//! concurrent committers do not wake in lockstep.
struct IcebergRetryBackoff {
	explicit IcebergRetryBackoff(const IcebergRetryConfig &config_p)
	    : config(config_p), prev_sleep_ms(config_p.min_wait_ms), start(std::chrono::steady_clock::now()),
	      rng(std::random_device {}() ^
	          static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())),
	      dist(0.0, 1.0) {
	}

	//! Sleep before the next attempt (attempt is the just-failed 0-based attempt index). Returns
	//! false if the wait would exceed commit.retry.total-timeout-ms (only after >=1 retry, mirroring
	//! Java's Tasks.runTaskWithRetry), signalling the caller to stop retrying.
	bool WaitBeforeRetry(idx_t attempt) {
		auto wait_ms = config.DecorrelatedBackoffMs(prev_sleep_ms, dist(rng));
		prev_sleep_ms = wait_ms;
		auto elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		if (attempt > 0 && (wait_ms > config.total_wait_ms || elapsed_ms > config.total_wait_ms - wait_ms)) {
			return false;
		}
		if (wait_ms > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
		}
		return true;
	}

	IcebergRetryConfig config;
	int64_t prev_sleep_ms;
	std::chrono::steady_clock::time_point start;
	std::mt19937_64 rng;
	std::uniform_real_distribution<double> dist;
};

} // namespace duckdb
