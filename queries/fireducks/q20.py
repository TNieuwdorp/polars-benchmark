from __future__ import annotations

from queries.fireducks import utils
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 20

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    part_ds = utils.get_part_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    nation_ds()
    part_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal part_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        nation_ds = nation_ds()
        part_ds = part_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        var1 = pd.Timestamp("1994-01-01")
        var2 = pd.Timestamp("1995-01-01")
        var3 = "CANADA"
        var4 = "forest"

        # Filter lineitem by shipdate and group by l_partkey and l_suppkey
        lineitem_grouped = (
            line_item_ds[(line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)]
            .groupby(["l_partkey", "l_suppkey"], as_index=False)["l_quantity"].sum()
            .assign(sum_quantity=lambda x: x["l_quantity"] * 0.5)
        )

        # Filter nation by n_name
        nation_filtered = nation_ds[nation_ds["n_name"] == var3]

        # Join supplier with filtered nation
        supplier_filtered = supplier_ds.merge(nation_filtered, left_on="s_nationkey", right_on="n_nationkey")

        # Filter part by p_name starting with var4
        part_filtered = part_ds[part_ds["p_name"].str.startswith(var4)]

        # Join part with partsupp, then join with lineitem_grouped
        merged_df = part_filtered.merge(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        merged_df = merged_df.merge(lineitem_grouped, left_on=["ps_suppkey", "p_partkey"], right_on=["l_suppkey", "l_partkey"])

        # Filter by ps_availqty > sum_quantity
        filtered_df = merged_df[merged_df["ps_availqty"] > merged_df["sum_quantity"]]

        # Select unique ps_suppkey and join with supplier_filtered
        unique_suppkey_df = filtered_df[["ps_suppkey"]].drop_duplicates()
        result_df = unique_suppkey_df.merge(supplier_filtered, left_on="ps_suppkey", right_on="s_suppkey")

        # Select relevant columns and sort by s_name
        result_df = result_df[["s_name", "s_address"]].sort_values(by="s_name")

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
