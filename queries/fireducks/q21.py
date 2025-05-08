from __future__ import annotations

from queries.fireducks import utils
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd

Q_NUM = 21

def q() -> None:
    line_item_ds = utils.get_line_item_ds
    nation_ds = utils.get_nation_ds
    orders_ds = utils.get_orders_ds
    supplier_ds = utils.get_supplier_ds

    # First call one time to cache in case we don't include the IO times
    line_item_ds()
    nation_ds()
    orders_ds()
    supplier_ds()

    def query() -> pd.DataFrame:
        nonlocal line_item_ds
        nonlocal nation_ds
        nonlocal orders_ds
        nonlocal supplier_ds

        line_item_ds = line_item_ds()
        nation_ds = nation_ds()
        orders_ds = orders_ds()
        supplier_ds = supplier_ds()

        var1 = "SAUDI ARABIA"

        # Suppliers from SAUDI ARABIA
        supplier_nation = supplier_ds.merge(nation_ds, left_on='s_nationkey', right_on='n_nationkey')
        supplier_saudi = supplier_nation[supplier_nation['n_name'] == var1][['s_suppkey', 's_name']]

        # Orders with status 'F'
        orders_f = orders_ds[orders_ds['o_orderstatus'] == 'F'][['o_orderkey']]

        # Lineitems with late shipments
        lineitem_late = line_item_ds[line_item_ds['l_receiptdate'] > line_item_ds['l_commitdate']][['l_orderkey', 'l_suppkey']]

        # All suppliers per order
        order_suppliers = line_item_ds[['l_orderkey', 'l_suppkey']].drop_duplicates()

        # Suppliers per order
        supplier_count = order_suppliers.groupby('l_orderkey').size().reset_index(name='supplier_count')

        # Late suppliers per order
        late_supplier_count = lineitem_late.groupby('l_orderkey').size().reset_index(name='late_supplier_count')

        # Combine counts
        counts = supplier_count.merge(late_supplier_count, on='l_orderkey', how='left').fillna(0)
        counts['late_supplier_count'] = counts['late_supplier_count'].astype(int)

        # Orders with more than one supplier and exactly one late supplier
        target_orders = counts[(counts['supplier_count'] > 1) & (counts['late_supplier_count'] == 1)]

        # Get the late supplier for these orders
        late_suppliers = lineitem_late[lineitem_late['l_orderkey'].isin(target_orders['l_orderkey'])]

        # Suppliers from SAUDI ARABIA who are late suppliers
        late_suppliers_saudi = late_suppliers.merge(supplier_saudi, left_on='l_suppkey', right_on='s_suppkey')

        # Join with orders to filter by status 'F'
        late_suppliers_saudi_f = late_suppliers_saudi.merge(orders_f, left_on='l_orderkey', right_on='o_orderkey')

        # Group and count
        result_df = late_suppliers_saudi_f.groupby('s_name').size().reset_index(name='numwait')
        result_df = result_df.sort_values(by=['numwait', 's_name'], ascending=[False, True]).head(100)

        return result_df

    utils.run_query(Q_NUM, query)

if __name__ == "__main__":
    q()
