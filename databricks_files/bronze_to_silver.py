from pyspark.sql import functions as F
from pyspark import pipelines as dp

MISSING_TOKENS = ['', 'unknown', 'nan', 'na', 'np', 'n/a', 'null', 'none']


def _clean_string(col, aliases=None):
    s = F.trim(col.cast("string"))
    s = F.when(F.lower(s).isin(MISSING_TOKENS), F.lit(None)).otherwise(s)
    s = F.initcap(s)
    if aliases:
        for old, new in aliases.items():
            old_title = old.strip().title()
            s = F.when(s == F.lit(old_title), F.lit(new)).otherwise(s)
    return s


def _clean_numeric(col, aliases=None):
    n = F.trim(col.cast("string"))
    n = F.when(F.lower(n).isin(MISSING_TOKENS), F.lit(None)).otherwise(n)
    n = F.regexp_replace(n, "%", "")
    n = F.regexp_replace(n, "\\$", "")
    n = F.regexp_replace(n, "₹", "")
    n = F.regexp_replace(n, "Rs\\.", "")
    n = F.regexp_replace(n, ",", "")
    n = F.trim(n)
    if aliases:
        for old, new in aliases.items():
            old_title = old.strip().title()
            n = F.when(n == F.lit(old_title), F.lit(str(new))).otherwise(n)
    return n.cast("double")


def _clean_date(col):
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col, "dd-MMM-yyyy"),
        F.to_date(col, "MMM dd, yyyy"),
        F.to_date(col, "yyyy/MM/dd"),
    )


@dp.materialized_view(
    name="silver_ecommerce_orders",
    comment="Cleaned e-commerce orders with standardized strings, typed numerics, and calculated columns"
)
def silver_ecommerce_orders():
    df = spark.read.table("workspace.messy_e_commerce.messy_ecommerce_orders")

    city_aliases = {
        'New Delhi': 'Delhi',
        'Bengaluru': 'Bangalore',
        'Bombay': 'Mumbai',
    }
    category_aliases = {
        'Home and Kitchen': 'Home & Kitchen',
        'Groceries': 'Grocery',
        'Electronic': 'Electronics',
        'Electronicss': 'Electronics',
    }
    quantity_aliases = {'two': '2'}
    order_status_aliases = {'cancel': 'Cancelled'}
    payment_method_aliases = {
        'cc': 'Credit Card',
        'dc': 'Debit Card',
        'upi': 'UPI',
        'cod': 'Cash On Delivery',
    }

    result = df.select(
        F.upper(_clean_string(df["order_id"])).alias("order_id"),
        _clean_date(df["order_date"]).alias("order_date"),
        F.upper(_clean_string(df["customer_id"])).alias("customer_id"),
        _clean_string(df["city"], aliases=city_aliases).alias("city"),
        _clean_string(df["category"], aliases=category_aliases).alias("category"),
        _clean_numeric(df["unit_price"]).alias("unit_price"),
        _clean_numeric(df["quantity"], aliases=quantity_aliases).cast("int").alias("quantity"),
        _clean_numeric(df["discount_pct"]).alias("discount_pct"),
        _clean_string(df["order_status"], aliases=order_status_aliases).alias("order_status"),
        _clean_string(df["payment_method"], aliases=payment_method_aliases).alias("payment_method"),
    )
    result = result.drop_duplicates(["order_id"])

    return result