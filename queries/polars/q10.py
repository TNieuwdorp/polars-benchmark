from datetime import date

import polars as pl

from queries.polars import utils

Q_NUM = 10


def q() -> None:
    var_1 = date(1993, 10, 1)
    var_2 = date(1994, 1, 1)

    customer_ds = utils.get_customer_ds()
    orders_ds = utils.get_orders_ds()
    line_item_ds = utils.get_line_item_ds()
    nation_ds = utils.get_nation_ds()

    q_final = (
        customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("l_returnflag") == "R")
        .group_by(
            "c_custkey",
            "c_name",
            "c_acctbal",
            "c_phone",
            "n_name",
            "c_address",
            "c_comment",
        )
        .agg(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .round(2)
            .alias("revenue")
        )
        .select(
            "c_custkey",
            "c_name",
            "revenue",
            "c_acctbal",
            "n_name",
            "c_address",
            "c_phone",
            "c_comment",
        )
        .sort(by="revenue", descending=True)
        .limit(20)
    )

    utils.run_query(Q_NUM, q_final)


if __name__ == "__main__":
    q()
