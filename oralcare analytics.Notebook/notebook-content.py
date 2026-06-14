# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5337b1ab-e62b-4dba-aa8f-84c756bd5689",
# META       "default_lakehouse_name": "lh_oralcare_analytics",
# META       "default_lakehouse_workspace_id": "1059b78f-0035-4ab6-903e-6bbfa55998e8",
# META       "known_lakehouses": [
# META         {
# META           "id": "5337b1ab-e62b-4dba-aa8f-84c756bd5689"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, trim, min as spark_min, max as spark_max, lower, upper, to_date, year, month, quarter, date_format,
    monotonically_increasing_id, current_timestamp, regexp_replace, sequence, explode, dayofmonth
    )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employees_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/employees/dim_employee.csv")
customers_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/customers/dim_customer.csv")
products_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/products/dim_product.csv")
territories_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/territories/dim_territory.csv")
sales_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/sales/fact_sales.csv")
prospective_orders_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/prospective_orders/fact_prospective_orders.csv")
targets_df = spark.read.option("header", True).option("inferSchema", True).csv("Files/bronze/raw/targets/fact_daily_sales_targets.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(targets_df.limit(10))
display(prospective_orders_df.limit(10))
display(sales_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employees_silver = (
    employees_df
    .withColumn("employee_key", col("employee_key").cast("int"))
    .withColumn("employee_id", trim(col("employee_id")))
    .withColumn("employee_name", trim(col("employee_name")))
    .withColumn("role", trim(col("role")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("manager_employee_key", col("manager_employee_key").cast("int"))
    .withColumn("manager_employee_name", trim(col("manager_employee_name")))
    .withColumn("territory_key", col("territory_key").cast("int"))
    .withColumn("employee_status", trim(col("employee_status")))

    .withColumn("commercial_manager_key", col("commercial_manager_key").cast("int"))
    .withColumn("commercial_manager_name", trim(col("commercial_manager_name")))
    .withColumn("regional_manager_key", col("regional_manager_key").cast("int"))
    .withColumn("regional_manager_name", trim(col("regional_manager_name")))
    .withColumn("sales_rep_key", col("sales_rep_key").cast("int"))
    .withColumn("sales_rep_name", trim(col("sales_rep_name")))
    .withColumn("business_region", trim(col("business_region")))

    .dropDuplicates(["employee_key"])
)

customers_silver = (
    customers_df
    .withColumn("customer_key", col("customer_key").cast("int"))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("customer_type", trim(col("customer_type")))
    .withColumn("channel", trim(col("channel")))
    .withColumn("city", trim(col("city")))
    .withColumn("territory_key", col("territory_key").cast("int"))
    .withColumn("assigned_employee_key", col("assigned_employee_key").cast("int"))
    .withColumn("customer_tier", trim(col("customer_tier")))
    .withColumn("customer_status", trim(col("customer_status")))
    .withColumn("onboarding_date", to_date(col("onboarding_date")))
    .dropDuplicates(["customer_key"])
)

products_silver = (
    products_df
    .withColumn("product_key", col("product_key").cast("int"))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("brand", trim(col("brand")))
    .withColumn("category", trim(col("category")))
    .withColumn("subcategory", trim(col("subcategory")))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("pack_size", trim(col("pack_size")))
    .withColumn("list_price_pln", col("list_price_pln").cast("double"))
    .withColumn("unit_cost_pln", col("unit_cost_pln").cast("double"))
    .withColumn("product_status", trim(col("product_status")))
    .dropDuplicates(["product_key"])
)

territories_silver = (
    territories_df
    .dropDuplicates(["territory_key"])
    .withColumn("territory_key", col("territory_key").cast("int"))
    .withColumn("country", trim(col("country")))
    .withColumn("voivodeship", trim(col("voivodeship")))
    .withColumn("city_district", trim(col("city_district")))
    .withColumn("business_region", trim(col("business_region")))
)

sales_silver = (
    sales_df
    .dropDuplicates(["invoice_id", "invoice_line_id"])
    .withColumn("invoice_id", col("invoice_id").cast("int"))
    .withColumn("invoice_line_id", col("invoice_line_id").cast("int"))
    .withColumn("customer_key", col("customer_key").cast("int"))
    .withColumn("product_key", col("product_key").cast("int"))
    .withColumn("employee_key", col("employee_key").cast("int"))
    .withColumn("territory_key", col("territory_key").cast("int"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price_pln", col("unit_price_pln").cast("double"))
    .withColumn("gross_amount_pln", col("gross_amount_pln").cast("double"))
    .withColumn("discount_amount_pln", col("discount_amount_pln").cast("double"))
    .withColumn("net_amount_pln", col("net_amount_pln").cast("double"))
    .withColumn("invoice_date", to_date(col("date_key").cast("string"), "yyyyMMdd"))
)

prospective_orders_silver = (
    prospective_orders_df
    .dropDuplicates(["order_id", "prospective_order_line_id"])
    .withColumn("order_id", col("order_id").cast("int"))
    .withColumn("prospective_order_line_id", col("prospective_order_line_id").cast("int"))
    .withColumn("customer_key", col("customer_key").cast("int"))
    .withColumn("product_key", col("product_key").cast("int"))
    .withColumn("employee_key", col("employee_key").cast("int"))
    .withColumn("territory_key", col("territory_key").cast("int"))
    .withColumn("expected_quantity", col("expected_quantity").cast("int"))
    .withColumn("expected_amount_pln", col("expected_amount_pln").cast("double"))
    .withColumn("expected_close_date", to_date(col("expected_close_date_key").cast("string"), "yyyyMMdd"))
    .withColumn("order_date", to_date(col("date_key").cast("string"), "yyyyMMdd"))
)

targets_silver = (
    targets_df
    .withColumn("date_key", col("date_key").cast("int"))
    .withColumn("full_date", to_date(col("full_date")))

    .withColumn("employee_key", col("employee_key").cast("int"))
    .withColumn("employee_name", trim(col("employee_name")))

    .withColumn("manager_employee_key", col("manager_employee_key").cast("int"))
    .withColumn("manager_employee_name", trim(col("manager_employee_name")))

    .withColumn("commercial_manager_key", col("commercial_manager_key").cast("int"))
    .withColumn("commercial_manager_name", trim(col("commercial_manager_name")))

    .withColumn("regional_manager_key", col("regional_manager_key").cast("int"))
    .withColumn("regional_manager_name", trim(col("regional_manager_name")))

    .withColumn("sales_rep_key", col("sales_rep_key").cast("int"))
    .withColumn("sales_rep_name", trim(col("sales_rep_name")))

    .withColumn("territory_key", col("territory_key").cast("int"))
    .withColumn("business_region", trim(col("business_region")))

    .withColumn("year", col("year").cast("int"))
    .withColumn("month", col("month").cast("int"))
    .withColumn("month_key", col("month_key").cast("int"))

    .withColumn("monthly_actual_sales_pln", col("monthly_actual_sales_pln").cast("double"))
    .withColumn("target_uplift_pct", col("target_uplift_pct").cast("double"))
    .withColumn("monthly_target_pln", col("monthly_target_pln").cast("double"))
    .withColumn("days_in_target_month", col("days_in_target_month").cast("int"))
    .withColumn("daily_target_pln", col("daily_target_pln").cast("double"))

    .dropDuplicates(["date_key", "employee_key"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employees_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("employees_silver")
customers_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("customers_silver")
products_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("products_silver")
territories_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("territories_silver")
sales_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("sales_silver")
prospective_orders_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("prospective_orders_silver")
targets_silver.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("targets_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_employee = (
    employees_silver
    .select(
        "employee_key",
        "employee_id",
        "employee_name",
        "role",
        "email",
        "manager_employee_key",
        "manager_employee_name",
        "commercial_manager_key",
        "commercial_manager_name",
        "regional_manager_key",
        "regional_manager_name",
        "sales_rep_key",
        "sales_rep_name",
        "territory_key",
        "business_region",
        "employee_status"
    )
    .dropDuplicates(["employee_key"])
)

dim_customer = (
    customers_silver
    .select(
        "customer_key",
        "customer_id",
        "customer_name",
        "customer_type",
        "channel",
        "city",
        "territory_key",
        "assigned_employee_key",
        "customer_tier",
        "customer_status",
        "onboarding_date"
    )
    .dropDuplicates(["customer_key"])
)

dim_product = (
    products_silver
    .select(
        "product_key",
        "product_id",
        "brand",
        "category",
        "subcategory",
        "product_name",
        "pack_size",
        "list_price_pln",
        "unit_cost_pln",
        "product_status"
    )
    .dropDuplicates(["product_key"])
)

dim_territory = territories_silver.select(
    "territory_key",
    "country",
    "voivodeship",
    "city_district",
    "business_region"
).dropDuplicates(["territory_key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_daily_sales_targets = (
    targets_silver
    .select(
        "date_key",
        "employee_key",
        "territory_key",
        "month_key",
        "monthly_actual_sales_pln",
        "target_uplift_pct",
        "monthly_target_pln",
        "days_in_target_month",
        "daily_target_pln"
    )
    .dropDuplicates(["date_key", "employee_key"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_dates = (
    sales_silver.select(col("invoice_date").alias("full_date"))
    .union(prospective_orders_silver.select(col("order_date").alias("full_date")))
    .union(prospective_orders_silver.select(col("expected_close_date").alias("full_date")))
    .union(targets_silver.select(col("full_date").alias("full_date")))
    .dropna(subset=["full_date"])
)

date_range = all_dates.agg(
    spark_min("full_date").alias("min_date"),
    spark_max("full_date").alias("max_date")
)

dim_date = (
    date_range
    .select(
        explode(sequence(col("min_date"), col("max_date"))).alias("full_date")
    )
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_number", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("year_month", date_format(col("full_date"), "yyyy-MM"))
    .withColumn("month_key", date_format(col("full_date"), "yyyyMM").cast("int"))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
    .dropDuplicates(["date_key"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = sales_silver.select(
    "invoice_line_id",
    "invoice_id",
    date_format(col("invoice_date"), "yyyyMMdd").cast("int").alias("date_key"),
    "customer_key",
    "product_key",
    "employee_key",
    "territory_key",
    "quantity",
    "unit_price_pln",
    "gross_amount_pln",
    "discount_amount_pln",
    "net_amount_pln"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_prospective_orders = prospective_orders_silver.select(
    "prospective_order_line_id",
    "order_id",
    date_format(col("order_date"), "yyyyMMdd").cast("int").alias("date_key"),
    date_format(col("expected_close_date"), "yyyyMMdd").cast("int").alias("expected_close_date_key"),
    "customer_key",
    "product_key",
    "employee_key",
    "territory_key",
    "expected_quantity",
    "expected_amount_pln",
    "probability"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dim_date")
dim_employee.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dim_employee")
dim_customer.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dim_customer")
dim_product.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dim_product")
dim_territory.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dim_territory")
fact_sales.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("fact_sales")
fact_prospective_orders.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("fact_prospective_orders")
fact_daily_sales_targets.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("fact_daily_sales_targets")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_sales.limit(5))
display(fact_prospective_orders.limit(5))
display(dim_product.limit(5))
display(dim_customer.limit(5))
display(dim_territory.limit(5))
display(dim_employee.limit(5))
display(fact_daily_sales_targets.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("fact_sales:", fact_sales.count())
print("dim_customer:", dim_customer.count())
print("dim_product:", dim_product.count())
print("dim_employee:", dim_employee.count())
print("dim_territory:", dim_territory.count())
print("fact_prospective_orders:", fact_prospective_orders.count())
print("fact_dail_sales_targets:", fact_daily_sales_targets.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing_products = fact_sales.join(
    dim_product,
    fact_sales["product_key"] == dim_product["product_key"],
    "left_anti"
)

missing_customers = fact_sales.join(
    dim_customer,
    fact_sales["customer_key"] == dim_customer["customer_key"],
    "left_anti"
)

missing_employees = fact_sales.join(
    dim_employee,
    fact_sales["employee_key"] == dim_employee["employee_key"],
    "left_anti"
)

missing_territories = fact_sales.join(
    dim_territory,
    fact_sales["territory_key"] == dim_territory["territory_key"],
    "left_anti"
)

print("Missing products:", missing_products.count())
print("Missing customers:", missing_customers.count())
print("Missing employees:", missing_employees.count())
print("Missing territories:", missing_territories.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
