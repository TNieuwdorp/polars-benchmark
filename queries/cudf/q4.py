from __future__ import annotations

from datetime import date

import cudf.pandas

cudf.pandas.install()
import pandas as pd
import numpy as np

from queries.cudf import utils

Q_NUM = 4


def q() -> None:
    line_item_ds = utils.get_line_item_ds
    orders_ds = utils.get_orders_ds

    # first call one time to cache in case we don't include the IO times
    line_item_ds()
    orders_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal orders_ds
        line_item_ds = line_item_ds()
        orders_ds = orders_ds()

        var1 = np.datetime64("1993-07-01")
        var2 = np.datetime64("1993-10-01")

        jn = line_item_ds.merge(orders_ds, left_on="l_orderkey", right_on="o_orderkey")

        jn = jn[(jn["o_orderdate"] >= var1) & (jn["o_orderdate"] < var2)]
        jn = jn[jn["l_commitdate"] < jn["l_receiptdate"]]

        jn = jn.drop_duplicates(subset=["o_orderpriority", "l_orderkey"])

        gb = jn.groupby("o_orderpriority", as_index=False)
        agg = gb.agg({'o_orderkey': 'count'}).rename(columns={'o_orderkey': 'order_count'})

        result_df = agg.sort_values(["o_orderpriority"])

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
