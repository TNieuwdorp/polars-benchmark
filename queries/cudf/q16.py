from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.pandas import utils

Q_NUM = 16

def q() -> None:
    part_ds = utils.get_part_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    part_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal part_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        part_ds = part_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        var1 = "Brand#45"

        # Filter supplier for comments containing "Customer" and "Complaints"
        supplier_filtered = supplier_ds[supplier_ds["s_comment"].str.contains(".*Customer.*Complaints.*")]
        supplier_filtered = supplier_filtered[["s_suppkey"]]
        supplier_filtered = supplier_filtered.rename(columns={"s_suppkey": "ps_suppkey"})

        # Join part and part_supp, then apply filters
        merged_df = part_ds.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        filtered_df = (
            merged_df[(merged_df["p_brand"] != var1) &
                      (~merged_df["p_type"].str.contains("MEDIUM POLISHED")) &
                      (merged_df["p_size"].isin([49, 14, 23, 45, 19, 3, 36, 9]))]
        )

        # Left join with supplier_filtered and filter out rows with matching ps_suppkey
        filtered_df = filtered_df.merge(supplier_filtered, on="ps_suppkey", how="left", indicator=True)
        filtered_df = filtered_df[filtered_df["_merge"] == "left_only"].drop(columns="_merge")

        # Group by "p_brand", "p_type", "p_size" and count unique suppliers
        result_df = (
            filtered_df
            .groupby(["p_brand", "p_type", "p_size"], as_index=False)
            .agg(supplier_cnt=pd.NamedAgg(column="ps_suppkey", aggfunc="nunique"))
            .sort_values(by=["supplier_cnt", "p_brand", "p_type", "p_size"], ascending=[False, True, True, True])
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
