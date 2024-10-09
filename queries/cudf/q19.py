from __future__ import annotations
import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.pandas import utils

Q_NUM = 19

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

        # Join part with lineitem
        merged_df = part_ds.merge(line_item_ds, left_on="p_partkey", right_on="l_partkey")

        # Apply filters based on conditions
        filtered_df = merged_df[
            (merged_df["l_shipmode"].isin(["AIR", "AIR REG"])) &
            (merged_df["l_shipinstruct"] == "DELIVER IN PERSON") &
            (
                ((merged_df["p_brand"] == "Brand#12") &
                 (merged_df["p_container"].isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])) &
                 (merged_df["l_quantity"].between(1, 11)) &
                 (merged_df["p_size"].between(1, 5))) |
                ((merged_df["p_brand"] == "Brand#23") &
                 (merged_df["p_container"].isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])) &
                 (merged_df["l_quantity"].between(10, 20)) &
                 (merged_df["p_size"].between(1, 10))) |
                ((merged_df["p_brand"] == "Brand#34") &
                 (merged_df["p_container"].isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])) &
                 (merged_df["l_quantity"].between(20, 30)) &
                 (merged_df["p_size"].between(1, 15)))
            )
        ]

        # Calculate revenue
        revenue = round((filtered_df["l_extendedprice"] * (1 - filtered_df["l_discount"])).sum(), 2)
        result_df = pd.DataFrame({"revenue": [revenue]})

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
