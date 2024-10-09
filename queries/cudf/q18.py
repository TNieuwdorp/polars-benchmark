from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.pandas import utils

Q_NUM = 18

def q() -> None:
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # First call one time to cache in case we don't include the IO times
    customer_ds()
    line_item_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal orders_ds

        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        var1 = 300

        # Group lineitem by "l_orderkey" and calculate sum of "l_quantity"
        sum_quantity_df = (
            line_item_ds.groupby("l_orderkey", as_index=False)["l_quantity"].sum()
            .rename(columns={"l_quantity": "sum_quantity"})
        )

        # Filter for sum_quantity > var1
        sum_quantity_df = sum_quantity_df[sum_quantity_df["sum_quantity"] > var1]

        # Semi-join orders with filtered lineitem
        filtered_orders = orders_ds[orders_ds["o_orderkey"].isin(sum_quantity_df["l_orderkey"])]

        # Join filtered orders with lineitem and customer
        merged_df = filtered_orders.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        merged_df = merged_df.merge(customer_ds, left_on="o_custkey", right_on="c_custkey")

        # Group by relevant columns and calculate sum of "l_quantity"
        grouped_df = (
            merged_df.groupby(["c_name", "o_custkey", "o_orderkey", "o_orderdate", "o_totalprice"], as_index=False)
            .agg(col6=pd.NamedAgg(column="l_quantity", aggfunc="sum"))
        )

        # Select relevant columns and sort by "o_totalprice" and "o_orderdate"
        result_df = (
            grouped_df
            .rename(columns={"o_orderdate": "o_orderdat"})
            .sort_values(by=["o_totalprice", "o_orderdat"], ascending=[False, True])
            .head(100)
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
