#Create a pipeline for the customers and transactions which are already sotred in raw_data volume storage.
# Which are in the form of jSON format
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
# Define variables for catalog and volume paths
catalog = "sales_cta"
schema = dbName = db = "db_salesdp_cdc"
volume_name = "raw_data"
volume_folder = f"/Volumes/{catalog}/{db}/{volume_name}"
table_name = ['customers','transactions']

# Assign the business rules
rules = {
    "rule1" : "id is NOT NULL",
}
@dlt.expect_all_or_drop(rules)
@dlt.table(
    name = f"{catalog}.bronze.customers_py_cdc",
    comment = "Data loads from raw_data valume storage"
)

def bronze_py_customers():
    return(
        spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "json")\
            .load(f'{volume_folder}/customers')
    )

##Clean CDC input and track quality with expectations
@dlt.temporary_view(
    name = f"{table_name[0]}_py_cdc_clean",
    comment = "Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IN ('APPEND', 'DELETE', 'UPDATE')")

def raw_cdc_clean():
    return spark.readStream.table(f"{catalog}.bronze.{table_name[0]}_py_cdc")

#Silver
@dlt.table(
    name = f"{catalog}.silver.customers_py_cdc_clean",
    comment = "Data loads from customers_py_cdc_clean to silver layer"
    )

def view_cdc_clean():
    return spark.readStream.table(f"{table_name[0]}_py_cdc_clean")


#Gold
##Materialize the final table
dlt.create_streaming_table(
        name=f"{catalog}.gold.{table_name[0]}_py", 
        comment="Clean, materialized " + table_name[0])
dlt.create_auto_cdc_flow(
        target=f"{catalog}.gold.{table_name[0]}_py",  # The customer table being materilized
        source=f"{catalog}.silver.customers_py_cdc_clean",  # the incoming CDC
        keys=["id"],  # what we'll be using to match the rows to upsert
        sequence_by=col("operation_date"),  # we deduplicate by operation date getting the most recent value
        ignore_null_updates=False,
        apply_as_deletes=expr("operation = 'DELETE'"),  # DELETE condition
        except_column_list=["operation", "operation_date", "_rescued_data"], # in addition we drop metadata columns
    )

# ---------------------------------------------------------------
# --- -- Slowly Changing Dimension of type 2 (SCD2) -------------
# ---------------------------------------------------------------
#Create a streaming table
dlt.create_streaming_table(
    name = f"{catalog}.gold.SCD2_customers_py",
    comment="Slowly Changing Dimension Type 2 for customers")

#Store all changes as SCD2
dlt.create_auto_cdc_flow(
    target = f"{catalog}.gold.SCD2_customers_py",
    source = f"{catalog}.silver.customers_py_cdc_clean",
    keys = ["id"],
    sequence_by = col("operation_date"),
    ignore_null_updates = False,
    apply_as_deletes = expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_date", "_rescued_data"],
    stored_as_scd_type="2",
)
