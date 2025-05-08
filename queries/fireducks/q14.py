from __future__ import annotations

from queries.fireducks import utils
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 14

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

        var1 = pd.Timestamp("1995-09-01")
        var2 = pd.Timestamp("1995-10-01")

        # Join lineitem and part
        merged_df = line_item_ds.merge(part_ds, left_on="l_partkey", right_on="p_partkey")

        # Filter where l_shipdate is between var1 and var2 (left-closed)
        filtered_df = merged_df[(merged_df["l_shipdate"] >= var1) & (merged_df["l_shipdate"] < var2)]

        # Calculate promo_revenue
        total_revenue = (filtered_df["l_extendedprice"] * (1 - filtered_df["l_discount"])).sum()
        promo_revenue = (
            100.00 * (
                filtered_df[filtered_df["p_type"].str.contains("PROMO")]["l_extendedprice"] *
                (1 - filtered_df["l_discount"])
            ).sum() / total_revenue
        )

        result_df = pd.DataFrame({"promo_revenue": [round(promo_revenue, 2)]})

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
