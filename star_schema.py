import sqlite3
import pandas as pd
import os

# Connect to the same database created in extract step
db_path = os.path.join(os.path.dirname(__file__), 'olist_growth_db.db')

with sqlite3.connect(db_path) as conn:

    # Get the dates from the database created in extract step
    orders_raw = pd.read_sql("SELECT * FROM orders", conn)

    # Convert date columns to datetime
    orders_raw['order_purchase_timestamp'] = pd.to_datetime(orders_raw['order_purchase_timestamp'])

    # Extract unique dates from order purchase timestamps
    dates = orders_raw['order_purchase_timestamp'].dt.normalize().drop_duplicates().sort_values()
    dates = dates.reset_index(drop=True)

    # Build the dimension table
    dim_date = pd.DataFrame({
        'date_id'   : dates.dt.strftime('%Y%m%d').astype(int),  # e.g. 20170101
        'date'      : dates.dt.date,
        'year'      : dates.dt.year,
        'quarter'   : dates.dt.quarter,
        'month'     : dates.dt.month,
        'month_name': dates.dt.strftime('%B'),
        'week'      : dates.dt.isocalendar().week.astype(int),
        'day'       : dates.dt.day,
        'weekday'   : dates.dt.weekday,         # 0=Monday, 6=Sunday
        'weekday_name': dates.dt.strftime('%A'),
        'is_weekend': dates.dt.weekday >= 5,
    })

    dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
    print(f"Dim_date loaded: {len(dim_date)} rows")


    # Build the fact table by joining order_items with orders and payments, and calculating delivery times and lateness
    # Load the raw tables we need
    order_items = pd.read_sql("SELECT * FROM order_items", conn)
    orders_raw  = pd.read_sql("SELECT * FROM orders", conn)
    payments    = pd.read_sql("SELECT * FROM payments", conn)

    # Convert dates
    orders_raw['order_purchase_timestamp']  = pd.to_datetime(orders_raw['order_purchase_timestamp'])
    orders_raw['order_delivered_customer_date'] = pd.to_datetime(orders_raw['order_delivered_customer_date'])
    orders_raw['order_estimated_delivery_date'] = pd.to_datetime(orders_raw['order_estimated_delivery_date'])

    # Aggregate payments per order (an order can have multiple payment rows)
    payments_agg = payments.groupby('order_id').agg(
        total_payment=('payment_value', 'sum'),
        payment_type =('payment_type',  'first'),
        installments =('payment_installments', 'max')
    ).reset_index()

    # Calculate delivery days and whether delivery was late
    orders_raw['delivery_days'] = (
        orders_raw['order_delivered_customer_date'] - orders_raw['order_purchase_timestamp']
    ).dt.days

    orders_raw['is_late'] = (
        orders_raw['order_delivered_customer_date'] > orders_raw['order_estimated_delivery_date']
    ).astype(int)

    # Add date_id foreign key (matches dim_date)
    orders_raw['date_id'] = orders_raw['order_purchase_timestamp'].dt.strftime('%Y%m%d').astype(int)

    # Merge everything into one fact table
    fact_orders = order_items.merge(orders_raw[[
        'order_id', 'customer_id', 'date_id',
        'order_status', 'delivery_days', 'is_late'
    ]], on='order_id', how='left')

    fact_orders = fact_orders.merge(payments_agg, on='order_id', how='left')

    # Keep only the columns we need
    fact_orders = fact_orders[[
        'order_id', 'order_item_id', 'customer_id', 'seller_id',
        'product_id', 'date_id', 'order_status',
        'price', 'freight_value', 'delivery_days', 'is_late',
        'total_payment', 'payment_type', 'installments'
    ]]

    fact_orders.to_sql('fact_orders', conn, if_exists='replace', index=False)
    print(f"Fact_orders loaded: {len(fact_orders)} rows")