from __future__ import annotations

import pandas as pd

from queries.pandas import utils

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

        # Join customer and orders
        merged_df = customer_ds.merge(orders_ds, left_on="c_custkey", right_on="o_custkey")
        # Join lineitem
        merged_df = merged_df.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        # Join nation
        merged_df = merged_df.merge(nation_ds, left_on="c_nationkey", right_on="n_nationkey")

        # Filter orders within the date range and l_returnflag == 'R'
        filtered_df = merged_df[
            (merged_df["o_orderdate"] >= var1) &
            (merged_df["o_orderdate"] < var2) &
            (merged_df["l_returnflag"] == "R")
        ]

        # Group by relevant columns and calculate revenue
        grouped_df = (
            filtered_df
            .groupby([
                "c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"
            ], as_index=False)
            .apply(lambda x: round((x["l_extendedprice"] * (1 - x["l_discount"])).sum(), 2)).reset_index().rename(columns={0: "revenue"})
        )

        # Select the relevant columns and sort by revenue
        result_df = (
            grouped_df
            .sort_values(by="revenue", ascending=False)
            .head(20)
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
