from __future__ import annotations

import pandas as pd

from queries.pandas import utils

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

        # Join part and partsupp
        merged_df = part_ds.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        # Join supplier
        merged_df = merged_df.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        # Join lineitem
        merged_df = merged_df.merge(line_item_ds, left_on=["p_partkey", "ps_suppkey"], right_on=["l_partkey", "l_suppkey"])
        # Join orders
        merged_df = merged_df.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        # Join nation
        merged_df = merged_df.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")

        # Filter p_name containing "green"
        filtered_df = merged_df[merged_df["p_name"].str.contains("green")]

        # Select the relevant columns and calculate "amount"
        filtered_df = filtered_df.assign(
            nation=filtered_df["n_name"],
            o_year=filtered_df["o_orderdate"].dt.year,
            amount=(
                filtered_df["l_extendedprice"] * (1 - filtered_df["l_discount"]) -
                filtered_df["ps_supplycost"] * filtered_df["l_quantity"]
            )
        )

        # Group by "nation" and "o_year" and aggregate "sum_profit"
        result_df = (
            filtered_df
            .groupby(["nation", "o_year"], as_index=False)
            .agg(sum_profit=pd.NamedAgg(column="amount", aggfunc=lambda x: round(x.sum(), 2)))
            .sort_values(by=["nation", "o_year"], ascending=[True, False])
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
