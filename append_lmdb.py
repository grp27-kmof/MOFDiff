import lmdb
import pickle

def append_lmdb(target_path, source_path):
    target_db = lmdb.open(
        target_path,
        map_size=1099511627776 * 2,
        subdir=False,
        meminit=False,
        map_async=True,
    )

    with target_db.begin() as txn:
        length_bytes = txn.get("length".encode("ascii"))
        start_idx = pickle.loads(length_bytes) if length_bytes else 0
    print(f"Target currently has {start_idx} entries. Appending from {source_path} ...")

    source_db = lmdb.open(source_path, subdir=False, readonly=True, lock=False)
    count = 0
    with source_db.begin() as src_txn:
        cursor = src_txn.cursor()
        txn = target_db.begin(write=True)
        for key, value in cursor:
            if key == b"length":
                continue
            data = pickle.loads(value)
            new_idx = start_idx + count
            txn.put(f"{new_idx}".encode("ascii"), pickle.dumps(data, protocol=-1))
            count += 1
            if count % 20000 == 0:
                txn.commit()
                txn = target_db.begin(write=True)
                print(f"  ... {count} entries appended")
        txn.commit()
    source_db.close()

    total_len = start_idx + count
    txn = target_db.begin(write=True)
    txn.put("length".encode("ascii"), pickle.dumps(total_len, protocol=-1))
    txn.commit()
    target_db.sync()
    target_db.close()
    print(f"Done. Target now has {total_len} total entries ({count} appended).")

if __name__ == "__main__":
    append_lmdb(
        target_path="data/lmdbs/data.lmdb",
        source_path="data/lmdbs/MetalOxo.lmdb",
    )
