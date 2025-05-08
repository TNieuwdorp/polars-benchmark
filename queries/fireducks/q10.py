from __future__ import annotations

from queries.fireducks import utils
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

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

        var1 = pd.Timestamp("1993-10-01")
        var2 = pd.Timestamp("1994-01-01")

        # Select necessary columns
        customer_cols = ["c_custkey", "c_name", "c_acctbal", "c_phone", "c_nationkey", "c_address", "c_comment"]
        orders_cols = ["o_orderkey", "o_custkey", "o_orderdate"]
        line_item_cols = ["l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"]
        nation_cols = ["n_nationkey", "n_name"]

        customer_ds_filtered = customer_ds[customer_cols]
        orders_filtered = orders_ds[(orders_ds["o_orderdate"] >= var1) & (orders_ds["o_orderdate"] < var2)][orders_cols]
        line_item_filtered = line_item_ds[line_item_ds["l_returnflag"] == "R"][line_item_cols]
        nation_ds_filtered = nation_ds[nation_cols]

        # Merge datasets
        merged_df = customer_ds_filtered.merge(orders_filtered, left_on="c_custkey", right_on="o_custkey")
        merged_df = merged_df.merge(line_item_filtered, left_on="o_orderkey", right_on="l_orderkey")
        merged_df = merged_df.merge(nation_ds_filtered, left_on="c_nationkey", right_on="n_nationkey")

        # Calculate revenue
        merged_df["revenue"] = merged_df["l_extendedprice"] * (1 - merged_df["l_discount"])
        merged_df["revenue"] = merged_df["revenue"].round(2)

        # Group by and aggregate
        grouped_df = merged_df.groupby(
            ["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"], as_index=False
        )["revenue"].sum()

        # Sort and select top 20
        result_df = grouped_df.sort_values(by="revenue", ascending=False).head(20)

        # Reorder columns
        result_df = result_df[['c_custkey', 'c_name', 'revenue', 'c_acctbal', 'n_name', 'c_address', 'c_phone', 'c_comment']]

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
