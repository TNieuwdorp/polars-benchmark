from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.pandas import utils

Q_NUM = 21

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    nation_ds()
    orders_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        nation_ds = nation_ds()
        orders_ds = orders_ds()
        supplier_ds = supplier_ds()

        var1 = "SAUDI ARABIA"

        # Group lineitem by l_orderkey and calculate count of l_suppkey per order
        lineitem_grouped = (
            line_item_ds.groupby("l_orderkey", as_index=False)["l_suppkey"].count()
            .rename(columns={"l_suppkey": "n_supp_by_order"})
        )

        # Filter for orders with more than 1 supplier and join with lineitem on condition
        lineitem_filtered = line_item_ds[line_item_ds["l_receiptdate"] > line_item_ds["l_commitdate"]]
        q1 = lineitem_grouped[lineitem_grouped["n_supp_by_order"] > 1].merge(lineitem_filtered, on="l_orderkey")

        # Join q1 with supplier, nation, and orders
        q_final = (
            q1.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        )

        # Apply additional filters and group by s_name
        q_final = q_final[
            (q_final["n_supp_by_order"] == 1) &
            (q_final["n_name"] == var1) &
            (q_final["o_orderstatus"] == "F")
        ]


        # Group by supplier name and count occurrences
        result_df = (
            q_final.groupby("s_name").size()
            .reset_index(name='numwait')
            .sort_values(by=["numwait", "s_name"], ascending=[False, True])
            .head(100)
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
