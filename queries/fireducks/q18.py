from __future__ import annotations

from queries.fireducks import utils
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

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

        # Aggregate lineitem quantities
        sum_quantity_df = (
            line_item_ds.groupby("l_orderkey", as_index=False)["l_quantity"].sum()
            .rename(columns={"l_quantity": "sum_quantity"})
        )

        # Filter orders with total quantity greater than var1
        high_quantity_orders = sum_quantity_df[sum_quantity_df["sum_quantity"] > var1]

        # Filter orders and select necessary columns
        filtered_orders = orders_ds[orders_ds["o_orderkey"].isin(high_quantity_orders["l_orderkey"])][
            ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"]
        ]

        # Select necessary columns from lineitem and customer
        line_item_filtered = line_item_ds[["l_orderkey", "l_quantity"]]
        customer_filtered = customer_ds[["c_custkey", "c_name"]]

        # Merge datasets
        merged_df = filtered_orders.merge(line_item_filtered, left_on="o_orderkey", right_on="l_orderkey")
        merged_df = merged_df.merge(customer_filtered, left_on="o_custkey", right_on="c_custkey")

        # Group by and aggregate
        grouped_df = (
            merged_df.groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"], as_index=False)
            .agg(col6=("l_quantity", "sum"))
        )

        # Rename and sort
        grouped_df.rename(columns={"o_orderdate": "o_orderdat"}, inplace=True)
        result_df = grouped_df.sort_values(by=["o_totalprice", "o_orderdat"], ascending=[False, True]).head(100)

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
