from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.pandas import utils

Q_NUM = 11

def q() -> None:
    nation_ds = utils.get_nation_ds
    part_supp_ds = utils.get_part_supp_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    nation_ds()
    part_supp_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal nation_ds
        nonlocal part_supp_ds
        nonlocal supplier_ds

        nation_ds = nation_ds()
        part_supp_ds = part_supp_ds()
        supplier_ds = supplier_ds()

        var1 = "GERMANY"
        var2 = 0.0001

        # Join partsupp and supplier
        merged_df = part_supp_ds.merge(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        # Join nation
        merged_df = merged_df.merge(nation_ds, left_on="s_nationkey", right_on="n_nationkey")

        # Filter where n_name == var1
        filtered_df = merged_df[merged_df["n_name"] == var1]

        # Calculate "tmp"
        tmp = round((filtered_df["ps_supplycost"] * filtered_df["ps_availqty"]).sum() * var2, 2)

        # Create a new column for the product of 'ps_supplycost' and 'ps_availqty'
        filtered_df['cost_times_qty'] = filtered_df['ps_supplycost'] * filtered_df['ps_availqty']

        # Group by "ps_partkey" and calculate "value"
        grouped_df = (
            filtered_df
            .groupby("ps_partkey", as_index=False)
            .agg(value=pd.NamedAgg(
                column='cost_times_qty',
                aggfunc=lambda x: round(x.sum(), 2)
            ))
        )

        # Filter where "value" > "tmp"
        result_df = grouped_df[grouped_df["value"] > tmp]

        # Select relevant columns and sort by "value"
        result_df = result_df.sort_values(by="value", ascending=False)[["ps_partkey", "value"]]

        return result_df


    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
