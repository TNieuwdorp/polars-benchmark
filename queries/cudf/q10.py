from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd
import numpy as np

from queries.cudf import utils

Q_NUM = 10

def q() -> None:
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds

    # First call one time to cache in case we don't include the IO times
    customer_ds()
    line_item_ds()
    nation_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal orders_ds

        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        nation_ds = nation_ds()
        orders_ds = orders_ds()

        var1 = np.datetime64("1993-10-01")
        var2 = np.datetime64("1994-01-01")

        # Filter orders within the date range
        orders_ds = orders_ds[
            (orders_ds["o_orderdate"] >= var1) &
            (orders_ds["o_orderdate"] < var2)
        ][["o_orderkey", "o_custkey"]]

        # Filter lineitem for l_returnflag == 'R'
        line_item_ds = line_item_ds[line_item_ds["l_returnflag"] == "R"][["l_orderkey", "l_extendedprice", "l_discount"]]

        # Merge customer and nation
        customer_ds = customer_ds.merge(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
        customer_ds = customer_ds[["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"]]

        # Join operations
        merged_df = orders_ds.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")
        merged_df = merged_df.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")

        # Calculate revenue
        merged_df["revenue"] = merged_df["l_extendedprice"] * (1 - merged_df["l_discount"])

        # Group by relevant columns and calculate revenue
        grouped_df = (
            merged_df
            .groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"], as_index=False)
            .agg({"revenue": "sum"})
            .assign(revenue=lambda df: df["revenue"].round(2))
            .sort_values(by="revenue", ascending=False)
            .head(20)
        )

        # Reorder columns to match expected output
        grouped_df = grouped_df[['c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment']]

        return grouped_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
