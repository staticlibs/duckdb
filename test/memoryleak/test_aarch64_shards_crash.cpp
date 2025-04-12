#include "catch.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

using namespace duckdb;

class spin_lock {
	std::atomic_flag locked = ATOMIC_FLAG_INIT;

public:
	void lock() {
		while (locked.test_and_set(std::memory_order_acquire)) {
			// std::this_thread::yield();
		}
	}
	void unlock() {
		locked.clear(std::memory_order_release);
	}
};

class test_conn_pool {
	spin_lock lock;
	std::vector<Connection> connections;
	std::mt19937 random;
	DuckDB db;

public:
	test_conn_pool(const std::string &db_path, uint32_t num_conn_threads, uint32_t num_db_worker_threads) {
		std::string abs_db_path = FileSystem::GetWorkingDirectory() + "/" + db_path;
		DBConfig config;
		config.SetOptionByName("threads", Value::BIGINT(num_db_worker_threads));
		this->db = DuckDB(abs_db_path, &config);
		std::random_device random_dev;
		auto seed = random_dev();
		this->random = std::mt19937(seed);

		for (size_t i = 0; i < num_conn_threads; i++) {
			Connection conn = Connection(db);
			conn.SetAutoCommit(false);
			conn.Commit();
			connections.push_back(std::move(conn));
		}
	}

	Connection get_connection() {
		lock.lock();
		std::uniform_int_distribution<std::mt19937::result_type> dist(0, connections.size() - 1);
		size_t idx = static_cast<size_t>(dist(random));
		auto conn = std::move(connections.at(idx));
		connections.erase(connections.begin() + idx);
		lock.unlock();
		return conn;
	}

	void return_connection(Connection conn) {
		lock.lock();
		connections.push_back(std::move(conn));
		lock.unlock();
	}
};

static void execute_query(Connection &connection, const std::string &query) {
	auto res = connection.Query(query);
	if (res->HasError()) {
		res->ThrowError();
	}
}

static void concurrent_write(test_conn_pool &conn_pool, uint32_t num_shards, uint32_t num_conn_threads,
                             uint32_t num_rows) {
	std::atomic_uint64_t write_count(0);

	std::cout << "Starting connection threads, count: " << num_conn_threads << std::endl;
	for (uint32_t i = 0; i < num_conn_threads; i++) {
		auto th = std::thread([&] {
			std::random_device random_dev;
			auto seed = random_dev();
			std::mt19937 random(seed);
			std::uniform_int_distribution<std::mt19937::result_type> dist(0, num_shards - 1);
			while (true) {
				Connection connection = conn_pool.get_connection();
				try {
					auto shard_id = dist(random);
					uint64_t row_id = write_count.fetch_add(1, std::memory_order_acq_rel) % num_rows;
					connection.BeginTransaction();
					execute_query(connection,
					              "update shard" + std::to_string(shard_id) +
					                  ".main.test set amount = amount + 1 where id = " + std::to_string(row_id));
					connection.Commit();
					conn_pool.return_connection(std::move(connection));
				} catch (const std::exception &e) {
					std::cerr << e.what() << std::endl;
					std::terminate();
				}
			}
		});
		th.detach();
	}

	while (true) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10000));
		std::cout << "Write count: " << write_count.load(std::memory_order_acquire) << std::endl;
	}
}

static void setup_shards(test_conn_pool &conn_pool, uint32_t num_shards, uint32_t num_rows) {
	Connection connection = conn_pool.get_connection();
	for (uint32_t i = 0; i < num_shards; i++) {
		std::cout << "Generating data for shard number: " << i << ", rows count: " << num_rows << std::endl;
		connection.BeginTransaction();
		execute_query(connection, "attach database 'shard" + std::to_string(i) + ".db' as shard" + std::to_string(i));
		execute_query(connection, "use shard" + std::to_string(i));
		execute_query(connection,
		              "create or replace table test (id bigint primary key, amount int, description varchar)");
		execute_query(connection, "insert into test SELECT range as id, cast(random() * " + std::to_string(num_rows) +
		                              " as bigint) as amount, "
		                              "repeat('x', 10) as description FROM range(" +
		                              std::to_string(num_rows) + ");");
		connection.Commit();
	}
	conn_pool.return_connection(std::move(connection));
}

TEST_CASE("Reproduce a crash that happens mostly on AArch64 when multiple shards are used", "[aarch64_crash]") {
	uint32_t num_cores = std::thread::hardware_concurrency();
	uint32_t num_shards = 3;
	uint32_t num_conn_threads = num_cores;
	uint32_t num_db_worker_threads = num_cores;
	uint32_t num_rows = 1000000;

	std::cout << std::endl;
	std::cout << "CPU cores: " << num_cores << std::endl;
	std::cout << "DB shards: " << num_shards << std::endl;
	std::cout << "Connection threads: " << num_conn_threads << std::endl;
	std::cout << "DB worker threads: " << num_db_worker_threads << std::endl;

	test_conn_pool conn_pool("test.db", num_conn_threads, num_db_worker_threads);
	setup_shards(conn_pool, num_shards, num_rows);
	concurrent_write(conn_pool, num_shards, num_conn_threads, num_rows);
}
