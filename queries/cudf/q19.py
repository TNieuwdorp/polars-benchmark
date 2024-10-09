from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import cudf
import pandas as pd

from queries.cudf import utils

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

        # Filter parts based on conditions
        conditions = [
            {
                "brand": "Brand#12",
                "containers": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"],
                "quantity_range": (1, 11),
                "size_range": (1, 5)
            },
            {
                "brand": "Brand#23",
                "containers": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"],
                "quantity_range": (10, 20),
                "size_range": (1, 10)
            },
            {
                "brand": "Brand#34",
                "containers": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"],
                "quantity_range": (20, 30),
                "size_range": (1, 15)
            },
        ]

        # Prepare part filters
        part_filters = []
        for cond in conditions:
            part_filter = (
                (part_ds["p_brand"] == cond["brand"]) &
                (part_ds["p_container"].isin(cond["containers"])) &
                (part_ds["p_size"].between(*cond["size_range"]))
            )
            part_filters.append(part_filter)

        part_filter = part_filters[0] | part_filters[1] | part_filters[2]
        filtered_parts = part_ds[part_filter][["p_partkey", "p_brand", "p_size", "p_container"]]

        # Filter lineitem
        line_item_ds = line_item_ds[
            (line_item_ds["l_shipmode"].isin(["AIR", "AIR REG"])) &
            (line_item_ds["l_shipinstruct"] == "DELIVER IN PERSON")
        ]

        # Merge operations
        merged_df = filtered_parts.merge(line_item_ds, left_on="p_partkey", right_on="l_partkey")

        # Apply final conditions
        final_conditions = (
            ((merged_df["p_brand"] == "Brand#12") &
             (merged_df["l_quantity"].between(1, 11))) |
            ((merged_df["p_brand"] == "Brand#23") &
             (merged_df["l_quantity"].between(10, 20))) |
            ((merged_df["p_brand"] == "Brand#34") &
             (merged_df["l_quantity"].between(20, 30)))
        )
        filtered_df = merged_df[final_conditions]

        # Calculate revenue
        revenue = round((filtered_df["l_extendedprice"] * (1 - filtered_df["l_discount"])).sum(), 2)
        result_df = cudf.DataFrame({"revenue": [revenue]})

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
