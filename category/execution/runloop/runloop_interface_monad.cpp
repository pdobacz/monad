// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <category/core/fiber/priority_pool.hpp>
#include <category/core/monad_exception.hpp>
#include <category/execution/ethereum/block_hash_buffer.hpp>
#include <category/execution/ethereum/block_hash_buffer/util.hpp>
#include <category/execution/ethereum/core/fmt/bytes_fmt.hpp>
#include <category/execution/ethereum/db/block_db.hpp>
#include <category/execution/ethereum/db/state_machine_init.hpp>
#include <category/execution/ethereum/db/trie_db.hpp>
#include <category/execution/ethereum/state2/block_state.hpp>
#include <category/execution/ethereum/state3/state.hpp>
#include <category/execution/ethereum/core/rlp/block_rlp.hpp>
#include <category/execution/monad/chain/eest_net.hpp>
#include <category/execution/monad/chain/monad_chain.hpp>
#include <category/execution/monad/chain/monad_devnet.hpp>
#include <category/execution/monad/chain/monad_mainnet.hpp>
#include <category/execution/monad/chain/monad_testnet.hpp>
#include <category/execution/monad/db/state_machine_init.hpp>
#include <category/execution/runloop/runloop_interface_monad.h>
#include <category/execution/runloop/runloop_monad.hpp>
#include <category/mpt/db.hpp>
#include <category/vm/vm.hpp>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include <quill/LogLevel.h>
#include <quill/Quill.h>
#include <quill/handlers/FileHandler.h>

#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

using namespace monad;
namespace fs = std::filesystem;

MONAD_ANONYMOUS_NAMESPACE_BEGIN

// Pin the io_uring SQPOLL thread outside the worker CPUs where it
// can; fall back to the highest available CPU on smaller hosts
// (IORING_SETUP_SQ_AFF rejects an out-of-range CPU with EINVAL).
unsigned const sq_thread_cpu =
    std::min(7u, std::max(1u, std::thread::hardware_concurrency()) - 1u);
quill::LogLevel const log_level = quill::LogLevel::Info;
unsigned const nthreads = 4;
unsigned const nfibers = 256;

unsigned const mainnet_chain_id = 143;
unsigned const devnet_chain_id = 20143;
unsigned const testnet_chain_id = 10143;

std::unique_ptr<MonadChain> monad_chain_from_chain_id(uint64_t chain_id)
{
    if (chain_id == mainnet_chain_id) {
        return std::make_unique<MonadMainnet>();
    }
    if (chain_id == devnet_chain_id) {
        return std::make_unique<MonadDevnet>();
    }
    if (chain_id == testnet_chain_id) {
        return std::make_unique<MonadTestnet>();
    }
    MONAD_ABORT("invalid chain id");
}

struct AccountOverride
{
    uint256_t balance;
};

struct MonadRunloopDbCache : public Db
{
    std::unordered_map<Address, AccountOverride> account_override;
    Db &inner;

    MonadRunloopDbCache(Db &inner_arg)
        : inner{inner_arg}
    {
    }

    virtual bool is_page_encoded() const override
    {
        return inner.is_page_encoded();
    }

    virtual std::optional<Account> read_account(Address const &address) override
    {
        auto acct = inner.read_account(address);
        auto const over_it = account_override.find(address);
        if (over_it == account_override.end()) {
            return acct;
        }
        Account ret;
        if (acct.has_value()) {
            ret = *acct;
        }
        auto const &over = over_it->second;
        ret.balance = over.balance;
        return ret;
    }

    virtual bytes32_t read_storage(
        Address const &address, Incarnation const incarnation,
        bytes32_t const &key) override
    {
        return inner.read_storage(address, incarnation, key);
    }

    virtual storage_page_t read_storage_page(
        Address const &address, Incarnation const incarnation,
        bytes32_t const &page_key) override
    {
        return inner.read_storage_page(address, incarnation, page_key);
    }

    virtual vm::SharedIntercode read_code(bytes32_t const &code_hash) override
    {
        return inner.read_code(code_hash);
    }

    virtual void set_block_and_prefix(
        uint64_t const block_number,
        bytes32_t const &block_id = bytes32_t{}) override
    {
        inner.set_block_and_prefix(block_number, block_id);
    }

    virtual void
    finalize(uint64_t const block_number, bytes32_t const &block_id) override
    {
        inner.finalize(block_number, block_id);
    }

    virtual void update_verified_block(uint64_t const block_number) override
    {
        inner.update_verified_block(block_number);
    }

    virtual void update_voted_metadata(
        uint64_t const block_number, bytes32_t const &block_id) override
    {
        inner.update_voted_metadata(block_number, block_id);
    }

    virtual void update_proposed_metadata(
        uint64_t const block_number, bytes32_t const &block_id) override
    {
        inner.update_proposed_metadata(block_number, block_id);
    }

    // Two-stage commit (vicky page-store API). The eest-runner never calls
    // set_balance, so account_override is always empty here; reads apply
    // overrides, and commit simply delegates to the wrapped Db.
    virtual void commit(
        bytes32_t const &block_id, CommitBuilder &builder,
        BlockHeader const &header, StateDeltas const &state_deltas,
        std::function<void(BlockHeader &)> populate_header_fn) override
    {
        inner.commit(
            block_id, builder, header, state_deltas, populate_header_fn);
    }

    virtual BlockHeader read_eth_header() override
    {
        return inner.read_eth_header();
    }

    virtual bytes32_t state_root() override
    {
        return inner.state_root();
    }

    virtual bytes32_t receipts_root() override
    {
        return inner.receipts_root();
    }

    virtual bytes32_t transactions_root() override
    {
        return inner.transactions_root();
    }

    virtual std::optional<bytes32_t> withdrawals_root() override
    {
        return inner.withdrawals_root();
    }

    virtual std::string print_stats() override
    {
        return inner.print_stats();
    }

    virtual uint64_t get_block_number() const override
    {
        return inner.get_block_number();
    }
};

struct MonadRunloopImpl
{
    std::unique_ptr<MonadChain> chain;
    fs::path ledger_dir;
    mpt::Db raw_db;
    TrieDb triedb;
    // Dual-db migration: a page-encoded secondary timeline (if activated
    // on the db) runs alongside the slot-encoded primary so a
    // MONAD_NINE->MONAD_NEXT transition can cross the page-encoding
    // boundary. Empty for single-encoding (pure pre-MIP-8 or pure MIP-8).
    std::optional<mpt::Db> secondary_raw_db;
    std::optional<TrieDb> secondary_triedb;
    MonadRunloopDbCache db;
    vm::VM vm;
    BlockHashBufferFinalized block_hash_buffer;
    fiber::PriorityPool priority_pool;
    uint64_t block_num;
    bool is_first_run;

    MonadRunloopImpl(
        std::unique_ptr<MonadChain> chain, char const *ledger_path,
        char const *db_path);
};

MonadRunloopImpl::MonadRunloopImpl(
    std::unique_ptr<MonadChain> chain_arg, char const *ledger_path,
    char const *db_path)
    : chain{std::move(chain_arg)}
    , ledger_dir{ledger_path}
    , raw_db{[&] {
        // The on-disk Db ctor constructs the StateMachine from the
        // persisted state_machine_kind via these registries, so they must
        // be registered first.
        register_ethereum_state_machines();
        register_monad_state_machines();
        return mpt::Db{mpt::OnDiskDbConfig{
            .append = true,
            .compaction = true,
            .rewind_to_latest_finalized = true,
            .rd_buffers = 8192,
            .wr_buffers = 32,
            .uring_entries = 128,
            .sq_thread_cpu = sq_thread_cpu,
            .dbname_paths = {fs::path{db_path}}}};
    }()}
    , triedb{raw_db}
    , db{triedb}
    , vm{}
    , block_hash_buffer{}
    , priority_pool{nthreads, nfibers}
    , block_num{1}
    , is_first_run{true}
{
    // Open the page-encoded secondary timeline for dual-write migration
    // mode (slot primary + page secondary), if it was activated on the db.
    bool const secondary_active =
        raw_db.timeline_active(mpt::timeline_id::secondary);
    LOG_INFO("dual-db: secondary timeline active = {}", secondary_active);
    if (secondary_active) {
        secondary_raw_db = raw_db.open_secondary_timeline();
        MONAD_ASSERT(secondary_raw_db.has_value());
        secondary_triedb.emplace(*secondary_raw_db);
        MONAD_ASSERT(
            secondary_triedb->is_page_encoded(),
            "secondary timeline must be page-encoded");
        LOG_INFO("dual-db: opened page-encoded secondary timeline");
    }

    if (triedb.get_root() == nullptr) {
        LOG_INFO("loading from genesis");
        GenesisState const genesis_state = chain->get_genesis_state();
        load_genesis_state(genesis_state, triedb);
        // Seed the secondary with the same genesis so dual-writes build on
        // a consistent page-encoded base.
        if (secondary_triedb.has_value()) {
            load_genesis_state(genesis_state, *secondary_triedb);
        }
    }
    else {
        LOG_INFO("loading from previous DB state");
    }

    uint64_t const init_block_num = triedb.get_block_number();
    uint64_t const start_block_num = init_block_num + 1;

    LOG_INFO("Init block number = {}", init_block_num);

    mpt::AsyncIOContext io_ctx{mpt::ReadOnlyOnDiskDbConfig{
        .sq_thread_cpu = sq_thread_cpu, .dbname_paths = {fs::path{db_path}}}};
    mpt::Db rodb{io_ctx};
    bool const have_headers = init_block_hash_buffer_from_triedb(
        rodb, start_block_num, block_hash_buffer);
    if (!have_headers) {
        BlockDb block_db{ledger_path};
        MONAD_ASSERT(chain->get_chain_id() == mainnet_chain_id);
        MONAD_ASSERT(init_block_hash_buffer_from_blockdb(
            block_db, start_block_num, block_hash_buffer));
    }
}

MonadRunloopImpl *to_impl(MonadRunloop *x)
{
    return reinterpret_cast<MonadRunloopImpl *>(x);
}

MonadRunloop *from_impl(MonadRunloopImpl *x)
{
    return reinterpret_cast<MonadRunloop *>(x);
}

Address to_address(MonadRunloopAddress const *a)
{
    return std::bit_cast<Address>(*a);
}

uint256_t to_uint256(MonadRunloopWord const *x)
{
    // Big-endian load (avoids intx::be, whose as_bytes/bswap clash with
    // `using namespace monad`). Only used by the unused get/set_balance FFI.
    uint256_t r{};
    for (auto const b : x->bytes) {
        r = (r << 8) | uint256_t{b};
    }
    return r;
}

MONAD_ANONYMOUS_NAMESPACE_END

MONAD_ANONYMOUS_NAMESPACE_BEGIN

void monad_runloop_init_logging()
{
    auto stdout_handler = quill::stdout_handler();
    stdout_handler->set_pattern(
        "%(time) [%(thread_id)] %(file_name):%(line_number) LOG_%(log_level)\t"
        "%(message)",
        "%Y-%m-%d %H:%M:%S.%Qns",
        quill::Timezone::GmtTime);
    quill::Config quill_cfg;
    quill_cfg.default_handlers.emplace_back(stdout_handler);
    quill::configure(quill_cfg);
    quill::start(true);

    quill::get_root_logger()->set_log_level(log_level);
}

MONAD_ANONYMOUS_NAMESPACE_END

extern "C" MonadRunloop *monad_runloop_new(
    uint64_t chain_id, char const *ledger_path, char const *db_path)
{
    monad_runloop_init_logging();
    return from_impl(new MonadRunloopImpl{
        monad_chain_from_chain_id(chain_id), ledger_path, db_path});
}

extern "C" MonadRunloop *monad_runloop_new_eest(
    char const *ledger_path, char const *db_path,
    char const *genesis_alloc_json, char const *genesis_block_rlp_hex,
    char const *revision_schedule)
{
    monad_runloop_init_logging();
    MONAD_ASSERT(genesis_alloc_json != nullptr);
    MONAD_ASSERT(genesis_block_rlp_hex != nullptr);
    MONAD_ASSERT(revision_schedule != nullptr);
    auto const genesis_block_rlp =
        evmc::from_hex(std::string_view{genesis_block_rlp_hex});
    MONAD_ASSERT(genesis_block_rlp.has_value());
    byte_string_view genesis_block_rlp_view{
        genesis_block_rlp->data(), genesis_block_rlp->size()};
    auto const genesis_block = rlp::decode_block(genesis_block_rlp_view);
    MONAD_ASSERT(!genesis_block.has_error());

    // Parse "<revision>:<from timestamp>,..." into the schedule.
    EestNetRevisionSchedule schedule;
    std::istringstream stream{revision_schedule};
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        auto const sep = entry.find(':');
        MONAD_ASSERT(sep != std::string::npos);
        auto const revision = std::stoul(entry.substr(0, sep));
        MONAD_ASSERT(revision <= MONAD_NEXT);
        auto const from_timestamp = std::stoull(entry.substr(sep + 1));
        schedule.emplace_back(
            from_timestamp, static_cast<monad_revision>(revision));
    }

    return from_impl(new MonadRunloopImpl{
        std::make_unique<EestNet>(
            std::string{genesis_alloc_json},
            genesis_block.assume_value().header,
            std::move(schedule)),
        ledger_path,
        db_path});
}

extern "C" void monad_runloop_delete(MonadRunloop *runloop)
{
    delete to_impl(runloop);
}

extern "C" void monad_runloop_run(MonadRunloop *pre_runloop, uint64_t nblocks)
try {
    MonadRunloopImpl *const runloop = to_impl(pre_runloop);

    auto const block_num_before = runloop->block_num;

    sig_atomic_t const stop = 0;
    auto const result = runloop_monad(
        *runloop->chain,
        runloop->ledger_dir,
        runloop->raw_db,
        runloop->db,
        runloop->vm,
        runloop->block_hash_buffer,
        runloop->priority_pool,
        runloop->block_num,
        runloop->block_num + nblocks - 1,
        stop,
        /* enable_tracing = */ false,
        /* secondary_db = */ runloop->secondary_triedb.has_value()
            ? &*runloop->secondary_triedb
            : nullptr,
        /* is_first_run = */ runloop->is_first_run);

    runloop->is_first_run = false;

    auto const block_num_after = runloop->block_num;

    if (MONAD_UNLIKELY(result.has_error())) {
        LOG_ERROR(
            "block {} failed with: {}",
            block_num_after,
            result.assume_error().message().c_str());
        MONAD_ABORT();
    }
    MONAD_ASSERT(block_num_after - block_num_before == nblocks);
}
catch (MonadException const &e) {
    e.print();
    std::terminate();
}

extern "C" void monad_runloop_set_balance(
    MonadRunloop *pre_runloop, MonadRunloopAddress const *raw_addr,
    MonadRunloopWord const *raw_bal)
{
    MonadRunloopImpl *const runloop = to_impl(pre_runloop);
    auto const addr = to_address(raw_addr);
    auto const bal = to_uint256(raw_bal);
    runloop->db.account_override[addr].balance = bal;
}

extern "C" void monad_runloop_get_balance(
    MonadRunloop *pre_runloop, MonadRunloopAddress const *raw_addr,
    MonadRunloopWord *result_balance)
{
    MonadRunloopImpl *const runloop = to_impl(pre_runloop);
    auto const addr = to_address(raw_addr);
    auto const acct = runloop->db.read_account(addr);
    uint256_t bal;
    if (acct) {
        bal = acct->balance;
    }
    // Big-endian store (see to_uint256).
    for (int i = 31; i >= 0; --i) {
        result_balance->bytes[i] = static_cast<uint8_t>(bal);
        bal >>= 8;
    }
}

extern "C" void monad_runloop_get_state_root(
    MonadRunloop *pre_runloop, MonadRunloopWord *result_state_root)
{
    MonadRunloopImpl *const runloop = to_impl(pre_runloop);
    // In dual-db migration mode the canonical final state_root is the
    // page-encoded secondary's (transition fixtures end post-fork in
    // MONAD_NEXT); the slot primary holds the pre-fork-encoded root.
    bytes32_t const root = runloop->secondary_triedb.has_value()
                               ? runloop->secondary_triedb->state_root()
                               : runloop->db.state_root();
    *result_state_root = std::bit_cast<MonadRunloopWord>(root);
}

extern "C" void monad_runloop_dump(MonadRunloop *pre_runloop)
{
    MonadRunloopImpl *const runloop = to_impl(pre_runloop);
    std::cout << runloop->triedb.to_json().dump(4) << std::endl;
}

extern "C" char *monad_runloop_dump_json(MonadRunloop *pre_runloop)
{
    MonadRunloopImpl *const runloop = to_impl(pre_runloop);
    auto const json = runloop->triedb.to_json().dump();
    return strdup(json.c_str());
}

extern "C" void monad_runloop_free_string(char *str)
{
    free(str);
}
