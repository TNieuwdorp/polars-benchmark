from __future__ import annotations

from queries.fireducks import utils
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 12

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds

        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        var1 = "MAIL"
        var2 = "SHIP"
        var3 = pd.Timestamp("1994-01-01")
        var4 = pd.Timestamp("1995-01-01")

        # Join orders and lineitem
        merged_df = orders_ds.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")

        # Filter conditions
        filtered_df = merged_df[
            (merged_df["l_shipmode"].isin([var1, var2])) &
            (merged_df["l_commitdate"] < merged_df["l_receiptdate"]) &
            (merged_df["l_shipdate"] < merged_df["l_commitdate"]) &
            (merged_df["l_receiptdate"] >= var3) & (merged_df["l_receiptdate"] < var4)
        ]

        # Add "high_line_count" and "low_line_count" columns
        filtered_df["high_line_count"] = filtered_df["o_orderpriority"].isin(["1-URGENT", "2-HIGH"]).astype(int)
        filtered_df["low_line_count"] = (~filtered_df["o_orderpriority"].isin(["1-URGENT", "2-HIGH"])).astype(int)

        # Group by "l_shipmode" and aggregate
        result_df = (
            filtered_df
            .groupby("l_shipmode", as_index=False)
            .agg(
                high_line_count=pd.NamedAgg(column="high_line_count", aggfunc="sum"),
                low_line_count=pd.NamedAgg(column="low_line_count", aggfunc="sum")
            )
            .sort_values(by="l_shipmode")
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
