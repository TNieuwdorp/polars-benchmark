from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import cudf
import pandas as pd

from queries.cudf import utils

Q_NUM = 9

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds
    part_ds = utils.get_part_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    nation_ds()
    orders_ds()
    part_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal orders_ds
        nonlocal part_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        nation_ds = nation_ds()
        orders_ds = orders_ds()
        part_ds = part_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        # Filter part for p_name containing "green"
        part_ds = part_ds[part_ds["p_name"].str.contains("green")][["p_partkey", "p_name"]]

        # Select necessary columns
        line_item_ds = line_item_ds[["l_orderkey", "l_partkey", "l_suppkey", "l_extendedprice", "l_discount", "l_quantity"]]
        part_supp_ds = part_supp_ds[["ps_partkey", "ps_suppkey", "ps_supplycost"]]
        supplier_ds = supplier_ds[["s_suppkey", "s_nationkey", "s_name"]]
        nation_ds = nation_ds[["n_nationkey", "n_name"]]
        orders_ds = orders_ds[["o_orderkey", "o_orderdate"]]

        # Merge operations
        merged_df = part_ds.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        merged_df = merged_df.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        merged_df = merged_df.merge(line_item_ds, left_on=["p_partkey", "ps_suppkey"], right_on=["l_partkey", "l_suppkey"])
        merged_df = merged_df.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        merged_df = merged_df.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")

        # Calculate "amount"
        merged_df["amount"] = (
            merged_df["l_extendedprice"] * (1 - merged_df["l_discount"]) -
            merged_df["ps_supplycost"] * merged_df["l_quantity"]
        )
        merged_df["o_year"] = merged_df["o_orderdate"].dt.year

        # Group by "nation" and "o_year" and aggregate "sum_profit"
        result_df = (
            merged_df
            .groupby(["n_name", "o_year"], as_index=False)
            .agg({'amount': 'sum'})
            .rename(columns={'amount': 'sum_profit'})
            .assign(sum_profit=lambda df: df["sum_profit"].round(2))
            .rename(columns={"n_name": "nation"})
            .sort_values(by=["nation", "o_year"], ascending=[True, False])
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
