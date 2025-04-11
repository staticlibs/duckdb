#include "catch.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

using namespace duckdb;

class test_conn_pool {
	std::mutex mtx;
	std::vector<Connection> connections;
	std::mt19937 random;
	DuckDB db;

public:
	test_conn_pool(const std::string &db_path, uint32_t size)
	    : db(duckdb::FileSystem::GetWorkingDirectory() + "/" + db_path) {
		std::random_device random_dev;
		auto seed = random_dev();
		this->random = std::mt19937(seed);
		for (size_t i = 0; i < size; i++) {
			Connection conn = Connection(db);
			conn.SetAutoCommit(false);
			conn.Commit();
			connections.push_back(std::move(conn));
		}
	}

	Connection get_connection() {
		std::lock_guard<std::mutex> guard(mtx);
		std::uniform_int_distribution<std::mt19937::result_type> dist(0, connections.size() - 1);
		size_t idx = static_cast<size_t>(dist(random));
		auto conn = std::move(connections.at(idx));
		connections.erase(connections.begin() + idx);
		return conn;
	}

	void return_connection(Connection conn) {
		std::lock_guard<std::mutex> guard(mtx);
		connections.push_back(std::move(conn));
	}
};

static void execute_query(Connection &connection, const std::string &query) {
	auto res = connection.Query(query);
	if (res->HasError()) {
		res->ThrowError();
	}
}

static uint32_t get_next(std::mutex &mtx, std::atomic_uint32_t &integer, uint32_t max) {
	uint32_t pre_inc = integer.fetch_add(1, std::memory_order_acq_rel);
	if (pre_inc >= max) {
		std::lock_guard<std::mutex> guard(mtx);
		uint32_t cur = integer.load(std::memory_order_acquire);
		if (cur >= max) {
			integer.store(0, std::memory_order_release);
			return 0;
		}
	}
	return pre_inc;
}

static void concurrent_write(test_conn_pool &conn_pool, uint32_t num_shards, uint32_t num_threads, uint32_t num_rows) {
	std::atomic_uint32_t atomic_integer(0);
	std::random_device random_dev;
	auto seed = random_dev();
	std::mt19937 random(seed);
	std::uniform_int_distribution<std::mt19937::result_type> dist(0, num_shards - 1);
	std::mutex increment_mtx;
	std::atomic_uint64_t write_count(0);
	std::atomic_bool write_failed(false);

	for (uint32_t i = 0; i < num_threads; i++) {
		auto th = std::thread([&] {
			while (!write_failed.load(std::memory_order_acquire)) {
				Connection connection = conn_pool.get_connection();
				try {
					auto shard_id = dist(random);
					uint64_t row_id = get_next(increment_mtx, atomic_integer, num_rows);
					connection.BeginTransaction();
					execute_query(connection,
					              "update shard" + std::to_string(shard_id) +
					                  ".main.test set amount = amount + 1 where id = " + std::to_string(row_id));
					connection.Commit();
					write_count.fetch_add(1, std::memory_order_acq_rel);
					conn_pool.return_connection(std::move(connection));
				} catch (const std::exception &e) {
					write_failed.store(true, std::memory_order_release);
					std::cout << e.what() << std::endl;
					break;
				}
			}
		});
		th.detach();
	}

	while (!write_failed.load(std::memory_order_acquire)) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10000));
		std::cout << "Write count: " << write_count.load(std::memory_order_acquire) << std::endl;
	}
}

static void setup_shards(test_conn_pool &conn_pool, uint32_t num_shards, uint32_t num_rows) {
	Connection connection = conn_pool.get_connection();
	for (uint32_t i = 0; i < num_shards; i++) {
		connection.BeginTransaction();
		execute_query(connection, "attach database 'shard" + std::to_string(i) + ".db' as shard" + std::to_string(i));
		execute_query(connection, "use shard" + std::to_string(i));
		execute_query(connection,
		              "create or replace table test (id bigint primary key, amount int, description varchar)");
		execute_query(connection, "insert into test SELECT range id, cast(random() * 100000 as bigint) as amount, "
		                          "repeat('x', 10) as description FROM range(" +
		                              std::to_string(num_rows) + ");");
		connection.Commit();
	}
	conn_pool.return_connection(std::move(connection));
}

TEST_CASE("Reproduce a crash that happens only on AArch64 when multiple shards are used", "[aarch64_crash]") {
	uint32_t number_of_cores = std::thread::hardware_concurrency();
	uint32_t num_shards = number_of_cores / 2;
	uint32_t num_threads = number_of_cores;
	uint32_t num_rows = 1000000;

	std::cout << std::endl;
	std::cout << "CPU cores: " << number_of_cores << std::endl;
	std::cout << "DB shards: " << num_shards << std::endl;
	std::cout << "Worker threads: " << num_threads << std::endl;

	test_conn_pool conn_pool("test.db", num_threads);
	setup_shards(conn_pool, num_shards, num_rows);
	concurrent_write(conn_pool, num_shards, num_threads, num_rows);
}
