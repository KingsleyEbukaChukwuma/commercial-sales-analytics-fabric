// Fabric notebook source

// METADATA ********************

// META {
// META   "kernel_info": {
// META     "name": "synapse_pyspark"
// META   },
// META   "dependencies": {
// META     "lakehouse": {
// META       "default_lakehouse": "5337b1ab-e62b-4dba-aa8f-84c756bd5689",
// META       "default_lakehouse_name": "lh_oralcare_analytics",
// META       "default_lakehouse_workspace_id": "1059b78f-0035-4ab6-903e-6bbfa55998e8",
// META       "known_lakehouses": [
// META         {
// META           "id": "5337b1ab-e62b-4dba-aa8f-84c756bd5689"
// META         }
// META       ]
// META     },
// META     "warehouse": {
// META       "default_warehouse": "7ff9fffb-d2f1-48d0-a45c-3cbd16f7997b",
// META       "known_warehouses": [
// META         {
// META           "id": "7ff9fffb-d2f1-48d0-a45c-3cbd16f7997b",
// META           "type": "Lakewarehouse"
// META         }
// META       ]
// META     }
// META   }
// META }

// CELL ********************

from pyspark.sql.functions import (
    col, trim, min as spark_min, max as spark_max, lower, upper, to_date, year, month, quarter, date_format,
    monotonically_increasing_id, current_timestamp, regexp_replace, sequence, explode, dayofmonth
    )



// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }
