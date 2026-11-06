//! Temporary test binary for roadmap Step 2's backend skeleton (layer L2).
//!
//! Run with:
//! `HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 cargo test -p helios-persistence --features cassandra --test cassandra_skeleton_tests`
//!
//! The tests live in `tests/cassandra/backend_skeleton.rs`. Once the shared
//! Cassandra test binary (`tests/cassandra_tests.rs`, layer L1) is on this
//! branch, it includes that module with `#[path]` and this file is deleted.
//! Until then they skip when `HFS_TEST_CASSANDRA_URL` is unset.

#![cfg(feature = "cassandra")]

#[path = "cassandra/backend_skeleton.rs"]
mod backend_skeleton;
