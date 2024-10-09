from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.pandas import utils

Q_NUM = 17

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    part_ds = utils.get_part_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    part_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal part_ds

        line_item_ds = line_item_ds()
        part_ds = part_ds()

        var1 = "Brand#23"
        var2 = "MED BOX"

        # Filter part for brand and container type
        filtered_part = part_ds[(part_ds["p_brand"] == var1) & (part_ds["p_container"] == var2)]

        # Left join part with lineitem
        merged_df = filtered_part.merge(line_item_ds, how="left", left_on="p_partkey", right_on="l_partkey")

        # Group by "p_partkey" and calculate average quantity
        avg_quantity_df = (
            merged_df.groupby("p_partkey", as_index=False)["l_quantity"].mean()
            .assign(avg_quantity=lambda x: 0.2 * x["l_quantity"])
            [["p_partkey", "avg_quantity"]]  # Select only the necessary columns
        )

        # Join back with merged_df to filter by l_quantity < avg_quantity
        merged_with_avg = merged_df.merge(avg_quantity_df, on="p_partkey")
        filtered_df = merged_with_avg[merged_with_avg["l_quantity"] < merged_with_avg["avg_quantity"]]

        # Calculate average yearly revenue
        avg_yearly = round(filtered_df["l_extendedprice"].sum() / 7.0, 2)
        result_df = pd.DataFrame({"avg_yearly": [avg_yearly]})


        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
