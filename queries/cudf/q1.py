from __future__ import annotations

from datetime import date
import numpy as np

import cudf.pandas
cudf.pandas.install()
import cudf  # Import cudf explicitly
import pandas as pd

from queries.cudf import utils

Q_NUM = 1

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    line_item_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        line_item_ds = line_item_ds()
        
        var1 = np.datetime64('1998-09-02')

        filt = line_item_ds[line_item_ds["l_shipdate"] <= var1]

        filt["disc_price"] = filt.l_extendedprice * (1.0 - filt.l_discount)
        filt["charge"] = (
            filt.l_extendedprice * (1.0 - filt.l_discount) * (1.0 + filt.l_tax)
        )

        # Define the aggregation dictionary
        agg_dict = {
            'l_quantity': ['sum', 'mean'],
            'l_extendedprice': ['sum', 'mean'],
            'disc_price': 'sum',
            'charge': 'sum',
            'l_discount': 'mean',
            'l_orderkey': 'count',
        }

        # Perform groupby and aggregation
        gb = filt.groupby(["l_returnflag", "l_linestatus"], as_index=False)
        agg = gb.agg(agg_dict)

        # Flatten and rename columns
        agg.columns = [
            '_'.join(col).strip('_') if isinstance(col, tuple) else col
            for col in agg.columns.values
        ]

        column_mapping = {
            'l_quantity_sum': 'sum_qty',
            'l_quantity_mean': 'avg_qty',
            'l_extendedprice_sum': 'sum_base_price',
            'l_extendedprice_mean': 'avg_price',
            'disc_price_sum': 'sum_disc_price',
            'charge_sum': 'sum_charge',
            'l_discount_mean': 'avg_disc',
            'l_orderkey_count': 'count_order',
        }

        agg = agg.rename(columns=column_mapping)

        result_df = agg.sort_values(["l_returnflag", "l_linestatus"])

        # Reorder columns to match expected output
        result_df = result_df[['l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price',
                               'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc',
                               'count_order']]

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
