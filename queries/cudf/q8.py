from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import cudf
import pandas as pd
import numpy as np

from queries.cudf import utils

Q_NUM = 8

def q() -> None:
    customer_ds = utils.get_customer_ds
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds
    part_ds = utils.get_part_ds
    region_ds = utils.get_region_ds
    supplier_ds = utils.get_supplier_ds

    # First call to cache data
    customer_ds()
    line_item_ds()
    nation_ds()
    orders_ds()
    part_ds()
    region_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal customer_ds
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal orders_ds
        nonlocal part_ds
        nonlocal region_ds
        nonlocal supplier_ds

        customer_ds = customer_ds()
        line_item_ds = line_item_ds()
        nation_ds = nation_ds()
        orders_ds = orders_ds()
        part_ds = part_ds()
        region_ds = region_ds()
        supplier_ds = supplier_ds()

        var1 = "BRAZIL"
        var2 = "AMERICA"
        var3 = "ECONOMY ANODIZED STEEL"
        var4 = np.datetime64("1995-01-01")
        var5 = np.datetime64("1996-12-31")

        # Filter region and nation n1
        region_ds = region_ds[region_ds["r_name"] == var2][["r_regionkey"]]
        n1 = nation_ds[nation_ds["n_regionkey"].isin(region_ds["r_regionkey"])][["n_nationkey"]]
        # Filter customers in nations in the region
        customer_ds = customer_ds[customer_ds["c_nationkey"].isin(n1["n_nationkey"])][["c_custkey"]]
        # Filter orders in date range and from customers in the region
        orders_ds = orders_ds[
            (orders_ds["o_orderdate"] >= var4) & (orders_ds["o_orderdate"] <= var5) &
            (orders_ds["o_custkey"].isin(customer_ds["c_custkey"]))
        ][["o_orderkey", "o_orderdate"]]
        # Filter part
        part_ds = part_ds[part_ds["p_type"] == var3][["p_partkey"]]
        # n2 is the supplier nation
        n2 = nation_ds[["n_nationkey", "n_name"]]
        # Supplier dataset
        supplier_ds = supplier_ds[["s_suppkey", "s_nationkey"]]

        # Merge operations
        jn1 = part_ds.merge(line_item_ds, left_on="p_partkey", right_on="l_partkey")[[
            "l_suppkey", "l_orderkey", "l_extendedprice", "l_discount", "l_partkey"
        ]]
        jn2 = jn1.merge(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        jn3 = jn2.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        jn4 = jn3.merge(n2, left_on="s_nationkey", right_on="n_nationkey")

        # Compute volume and o_year
        jn4["o_year"] = jn4["o_orderdate"].dt.year
        jn4["volume"] = jn4["l_extendedprice"] * (1.0 - jn4["l_discount"])
        jn4 = jn4.rename(columns={"n_name": "nation"})

        # Calculate total volume per year
        total_volume = jn4.groupby("o_year")["volume"].sum().reset_index(name='total_volume')

        # Filter for nation == var1 (BRAZIL)
        brazil_volume = jn4[jn4["nation"] == var1].groupby("o_year")["volume"].sum().reset_index(name='brazil_volume')

        # Merge and compute market share
        result_df = total_volume.merge(brazil_volume, on='o_year', how='left').fillna(0)
        result_df["mkt_share"] = (result_df["brazil_volume"] / result_df["total_volume"]).round(2)
        result_df = result_df[["o_year", "mkt_share"]].sort_values("o_year")

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
