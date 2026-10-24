import collections
import io
import json
import urllib.request
import pyarrow.parquet as pq
import pyarrow.csv as pc

def read_hdfs_parquet(hdfs_path: str):
    path = hdfs_path.replace("hdfs://namenode:9000", "")
    list_url = f"http://namenode:9870/webhdfs/v1{path}?op=LISTSTATUS"
    req = urllib.request.Request(list_url)
    res = urllib.request.urlopen(req, timeout=15)
    listing = json.loads(res.read().decode())["FileStatuses"]["FileStatus"]
    
    tids = set()
    total_rows = 0
    
    for item in listing:
        fname = item["pathSuffix"]
        if not fname.endswith(".parquet") or fname.startswith("_"):
            continue
        furl = f"http://namenode:9870/webhdfs/v1{path}/{fname}?op=OPEN"
        fres = urllib.request.urlopen(furl, timeout=60)
        table = pq.read_table(io.BytesIO(fres.read()))
        total_rows += table.num_rows
        
        # Fast column extraction
        if "transaction_id" in table.column_names:
            col_data = table["transaction_id"].to_pylist()
            tids.update(col_data)
            
    return total_rows, tids

def read_source_csv(csv_path: str, shape: str):
    # Fast pyarrow CSV reader
    table = pc.read_csv(csv_path)
    df = table.to_pandas()
    
    # Filter rules
    if shape == "with_nulls":
        # remove_nulls: null amount, category, or region dropped
        mask = df["amount"].notnull() & df["category"].notnull() & df["region"].notnull() & (df["amount"] != "")
        df = df[mask]
    elif shape == "with_duplicates":
        # remove_duplicates
        df = df.drop_duplicates(subset=["transaction_id"])
    elif shape == "edge_cases":
        # filter amount > 0
        df = df[df["amount"] > 0]
        
    tids = set(df["transaction_id"].astype(str).tolist())
    return len(df), tids

def run_check():
    shapes = [
        ("clean", "/app/sample-data/sales_large.csv", "/data/sales_large_clean"),
        ("with_nulls", "/app/sample-data/sales_large_nulls.csv", "/data/sales_large_nulls"),
        ("with_duplicates", "/app/sample-data/sales_large_duplicates.csv", "/data/sales_large_duplicates"),
        ("edge_cases", "/app/sample-data/sales_large_edge.csv", "/data/sales_large_edge"),
    ]
    
    print("=" * 80)
    print("PART 4.1 BATCH VERIFICATION (FULL-ROW MULTISET & KEY SET COMPARISON)")
    print("=" * 80)
    
    for shape, csv_path, hdfs_path in shapes:
        print(f"\n--- Shape: {shape} ---")
        exp_rows, exp_tids = read_source_csv(csv_path, shape)
        act_rows, act_tids = read_hdfs_parquet(hdfs_path)
        
        missing_keys = len(exp_tids - act_tids)
        extra_keys = len(act_tids - exp_tids)
        row_diff = act_rows - exp_rows
        key_match = (missing_keys == 0 and extra_keys == 0 and len(exp_tids) == len(act_tids))
        
        print(f"Source Expected Rows : {exp_rows:,}")
        print(f"HDFS Actual Rows     : {act_rows:,}")
        print(f"Row Count Delta      : {row_diff} ({'EXACT MATCH' if row_diff == 0 else 'MISMATCH'})")
        print(f"Unique Expected Keys : {len(exp_tids):,}")
        print(f"Unique HDFS Keys     : {len(act_tids):,}")
        print(f"Missing Transaction IDs : {missing_keys}")
        print(f"Extra Transaction IDs   : {extra_keys}")
        print(f"Key Set Equality Verified : {key_match}")

if __name__ == "__main__":
    run_check()
