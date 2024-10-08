from __future__ import annotations

import pandas as pd

from queries.pandas import utils

Q_NUM = 13

def q() -> None:
    customer_ds = utils.get_customer_ds
    orders_ds = utils.get_orders_ds

    # First call one time to cache in case we don't include the IO times
    customer_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal customer_ds
        nonlocal orders_ds

        customer_ds = customer_ds()
        orders_ds = orders_ds()

        var1 = "special"
        var2 = "requests"

        # Filter orders where o_comment does not contain "special" followed by "requests"
        filtered_orders = orders_ds[~orders_ds["o_comment"].str.contains(f"{var1}.*{var2}")]

        # Left join customer with filtered orders
        merged_df = customer_ds.merge(filtered_orders, left_on="c_custkey", right_on="o_custkey", how="left")

        # Group by "c_custkey" and count "o_orderkey"
        grouped_df = (
            merged_df
            .groupby("c_custkey", as_index=False)
            .agg(c_count=pd.NamedAgg(column="o_orderkey", aggfunc="count"))
        )

        # Group by "c_count" and count occurrences
        custdist_df = (
            grouped_df["c_count"].value_counts()
            .reset_index(name='custdist')  # Correctly name the counts column
            .rename(columns={"index": "c_count"})  # Rename 'index' to 'c_count'
            .sort_values(by=["custdist", "c_count"], ascending=[False, False])
        )

        return custdist_df


    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()