from __future__ import annotations

import cudf.pandas

cudf.pandas.install()
import pandas as pd

from queries.cudf import utils

Q_NUM = 22

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

        # Add country code column by slicing first two characters of c_phone
        q1 = customer_ds.copy()
        q1["cntrycode"] = q1["c_phone"].str.slice(0, 2)
        q1 = q1[q1["cntrycode"].isin(["13", "31", "23", "29", "30", "18", "17"])]
        q1 = q1[["c_acctbal", "c_custkey", "cntrycode"]]

        # Calculate average account balance for customers with positive c_acctbal
        q2 = q1[q1["c_acctbal"] > 0.0]["c_acctbal"].mean()

        # Select unique customer keys from orders
        q3 = orders_ds["o_custkey"].drop_duplicates().to_frame(name="c_custkey")

        # Filter q1 for c_custkeys not in q3
        q_final = q1[~q1["c_custkey"].isin(q3["c_custkey"])]
        q_final = q_final[q_final["c_acctbal"] > q2]

        # Group by country code and aggregate numcust and totacctbal
        grouped = q_final.groupby("cntrycode", as_index=False).agg({"c_acctbal": ["count", "sum"]})

        # Flatten MultiIndex columns
        grouped.columns = ['cntrycode', 'numcust', 'totacctbal']

        # Round 'totacctbal'
        grouped['totacctbal'] = grouped['totacctbal'].round(2)

        # Sort by 'cntrycode'
        result_df = grouped.sort_values(by="cntrycode")

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
