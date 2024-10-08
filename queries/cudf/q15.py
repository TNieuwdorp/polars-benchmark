from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd
import numpy as np

from queries.cudf import utils

Q_NUM = 15

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    supplier_ds()
    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        supplier_ds = supplier_ds()

        var1 = np.datetime64("1996-01-01")
        var2 = np.datetime64("1996-04-01")

        # Filter the DataFrame based on ship date
        filtered_line_item_ds = line_item_ds[
            (line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)
        ]

        # Create a new 'revenue' column
        filtered_line_item_ds["revenue"] = (
            filtered_line_item_ds["l_extendedprice"] * (1 - filtered_line_item_ds["l_discount"])
        )

        # Calculate total revenue for each supplier
        revenue_df = filtered_line_item_ds.groupby("l_suppkey", as_index=False).agg({"revenue": "sum"})
        revenue_df = revenue_df.rename(columns={"revenue": "total_revenue", "l_suppkey": "supplier_no"})

        # Join supplier with revenue and filter for max total_revenue
        merged_df = supplier_ds.merge(
            revenue_df, left_on="s_suppkey", right_on="supplier_no"
        )
        max_revenue = merged_df["total_revenue"].max()
        filtered_df = merged_df[merged_df["total_revenue"] == max_revenue]

        # Round total_revenue and select relevant columns
        filtered_df["total_revenue"] = filtered_df["total_revenue"].round(2)
        result_df = filtered_df[
            ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
        ].sort_values(by="s_suppkey")

        return result_df


    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
