from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import cudf
import pandas as pd

from queries.cudf import utils

Q_NUM = 21

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # First call to cache data
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

        # Filter nation
        nation_ds = nation_ds[nation_ds["n_name"] == var1][["n_nationkey"]]

        # Filter suppliers from SAUDI ARABIA
        supplier_ds = supplier_ds[supplier_ds["s_nationkey"].isin(nation_ds["n_nationkey"])][["s_suppkey", "s_name"]]

        # Filter orders with status 'F'
        orders_ds = orders_ds[orders_ds["o_orderstatus"] == "F"][["o_orderkey"]]

        # Merge lineitem with orders
        line_item_orders = line_item_ds.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")[[
            "l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate"
        ]]

        # Merge with suppliers
        line_item_suppliers = line_item_orders.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey", how='left')

        # Identify l1: lineitems where l_receiptdate > l_commitdate
        l1 = line_item_suppliers[line_item_suppliers["l_receiptdate"] > line_item_suppliers["l_commitdate"]]

        # Orders with lineitems where l_receiptdate > l_commitdate (l1)
        l1_orders = l1["l_orderkey"].drop_duplicates()

        # Identify orders that have multiple suppliers (EXISTS condition)
        order_supplier_counts = line_item_orders.groupby("l_orderkey")["l_suppkey"].nunique().reset_index()
        orders_with_multiple_suppliers = order_supplier_counts[order_supplier_counts["l_suppkey"] > 1]["l_orderkey"]

        # Candidate orders satisfying both l1 and multiple suppliers
        candidate_orders = l1_orders[l1_orders.isin(orders_with_multiple_suppliers)]

        # Apply the NOT EXISTS condition
        l3 = line_item_orders[
            (line_item_orders["l_orderkey"].isin(candidate_orders)) &
            (line_item_orders["l_receiptdate"] > line_item_orders["l_commitdate"])
        ]

        l3_supplier_counts = l3.groupby("l_orderkey")["l_suppkey"].nunique().reset_index()
        orders_to_exclude = l3_supplier_counts[l3_supplier_counts["l_suppkey"] > 1]["l_orderkey"]

        # Valid orders are candidate_orders excluding orders_to_exclude
        valid_orders = candidate_orders[~candidate_orders.isin(orders_to_exclude)]

        # Filter line items for valid orders
        valid_line_items = l1[l1["l_orderkey"].isin(valid_orders)]

        # Filter suppliers from SAUDI ARABIA
        valid_line_items = valid_line_items[valid_line_items["s_name"].notnull()]

        # Group by supplier name and count
        result_df = (
            valid_line_items.groupby("s_name")
            .size()
            .reset_index(name='numwait')
            .sort_values(by=["numwait", "s_name"], ascending=[False, True])
            .head(100)
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
