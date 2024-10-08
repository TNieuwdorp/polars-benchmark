from __future__ import annotations

import pandas as pd

from queries.pandas import utils

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

        var1 = pd.Timestamp("1996-01-01")
        var2 = pd.Timestamp("1996-04-01")

        # Calculate revenue for each supplier
        revenue_df = (
            line_item_ds[(line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)]
            .groupby("l_suppkey", as_index=False)
            .agg(total_revenue=pd.NamedAgg(
                column=(line_item_ds["l_extendedprice"] * (1 - line_item_ds["l_discount"])),
                aggfunc="sum"
            ))
        )
        revenue_df = revenue_df.rename(columns={"l_suppkey": "supplier_no"})

        # Join supplier with revenue and filter for max total_revenue
        merged_df = supplier_ds.merge(revenue_df, left_on="s_suppkey", right_on="supplier_no")
        max_revenue = merged_df["total_revenue"].max()
        filtered_df = merged_df[merged_df["total_revenue"] == max_revenue]

        # Round total_revenue and select relevant columns
        filtered_df["total_revenue"] = filtered_df["total_revenue"].round(2)
        result_df = filtered_df[["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]].sort_values(by="s_suppkey")

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()