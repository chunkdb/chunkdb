# Concurrency and Integrity Guarantees

## 1. Locking Model

- Global large-chunk map: guarded by `std::mutex`.
- Each large chunk owns a regular-chunk map: guarded by `std::mutex`.
- Each regular chunk payload uses `std::shared_mutex`:
  - `GET` / `CHUNK`: shared lock
  - `SET`: unique lock

## 2. Lock Ordering Rules

To avoid deadlocks, lock order is fixed:
1. Global large-chunk map mutex
2. Large-chunk internal mutex
3. Regular-chunk shared/unique mutex

No operation acquires more than one regular chunk payload lock at a time.

## 3. Consistency Under Concurrent Access

- Writes to the same regular chunk are serialized.
- Reads on the same regular chunk can run concurrently.
- Operations on different regular chunks can proceed in parallel.

## 4. Failure Semantics

For a single `SET`:
- Data is first staged in memory under chunk exclusive lock.
- Persist path uses WAL + atomic file replacement.
- If persistence fails, in-memory payload is rolled back before returning error.

## 5. Integrity Guarantees

- No partially written chunk image is accepted as valid state.
- WAL recovery guarantees restart consistency after crash between WAL and data file updates.
- CRC32 protects payload integrity against corruption/torn writes.
