import lmdb
import pickle
from pathlib import Path

def read_all_entries(lmdb_path):
    env = lmdb.open(str(lmdb_path), subdir=False, readonly=True, lock=False)
    entries = []
    with env.begin() as txn:
        cursor = txn.cursor()
        for key, value in cursor:
            if key == b"length":
                continue
            entries.append(pickle.loads(value))
    env.close()
    return entries

def merge_lmdbs(lmdb_paths, save_path):
    save_path = Path(save_path)
    save_path.mkdir(exist_ok=True, parents=True)
    out_db = lmdb.open(
        str(save_path / "data.lmdb"),
        map_size=1099511627776 * 2,
        subdir=False,
        meminit=False,
        map_async=True,
    )

    total_len = 0
    for lmdb_path in lmdb_paths:
        print(f"Reading {lmdb_path} ...")
        entries = read_all_entries(lmdb_path)
        print(f"  -> {len(entries)} entries found")
        txn = out_db.begin(write=True)
        for data in entries:
            txn.put(f"{total_len}".encode("ascii"), pickle.dumps(data, protocol=-1))
            total_len += 1
        txn.commit()

    txn = out_db.begin(write=True)
    txn.put("length".encode("ascii"), pickle.dumps(total_len, protocol=-1))
    txn.commit()
    out_db.sync()
    out_db.close()
    print(f"Merged LMDB written to {save_path / 'data.lmdb'} with {total_len} total entries.")

if __name__ == "__main__":
    merge_lmdbs(
        lmdb_paths=[
            "data/lmdbs/data.lmdb",
            "data/lmdbs/MetalOxo.lmdb",
        ],
        save_path="data/lmdbs_merged",
    )
