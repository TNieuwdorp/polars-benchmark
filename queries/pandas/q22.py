from __future__ import annotations

import pandas as pd

from queries.pandas import utils

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

        # Left join q1 with q3 and filter for customers without orders and with c_acctbal > avg_acctbal
        q_final = q1.merge(q3, on="c_custkey", how="left", indicator=True)
        q_final = q_final[q_final["_merge"] == "left_only"].drop(columns="_merge")
        q_final = q_final[q_final["c_acctbal"] > q2]

        # Group by country code and aggregate numcust and totacctbal
        result_df = (
            q_final.groupby("cntrycode", as_index=False)
            .agg(numcust=("c_acctbal", "count"), totacctbal=("c_acctbal", "sum"))
            .assign(totacctbal=lambda x: x["totacctbal"].round(2))
            .sort_values(by="cntrycode")
        )

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()