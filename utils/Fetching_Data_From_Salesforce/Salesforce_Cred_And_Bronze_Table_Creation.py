# Databricks notebook source
%pip install simple-salesforce

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

 
from simple_salesforce import Salesforce
 
 
# Salesforce OAuth credentials
consumer_key = "3MVG9GCMQoQ6rpzRROsBNkFzLInb_dRzDfgymX82mcg_NGYZknBtjtmmjzu1ylvkiP_hRs1l_QqX3X27n.uZP"
consumer_secret = "8067B454719CA47B57322BB525ACFC90B1067B929F4D5851A998D50605E61B7C"
 
 
# Salesforce login details
username = "swabarna.banerjee@cloudkaptan.com"
password = "Swabarna24BVYDIiH4v3K7lBY4aqCsEC1Fz"

# COMMAND ----------


print(username,password,consumer_key,consumer_secret)

sfc = Salesforce(username=username, password=password, security_token= None, consumer_key=consumer_key, consumer_secret=consumer_secret, domain='login')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE BRONZE;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
 
# Get all fields for the Account object using Metadata API
account_fields = sfc.Account.describe()["fields"]

# Construct the field list
field_names = [field["name"] for field in account_fields]
fields = ", ".join(field_names)
 
# Execute the query to fetch fields
query = f"SELECT {fields} FROM account"
result = sfc.query_all(query)
records = result["records"]
 
# Create SparkSession
spark = SparkSession.builder.getOrCreate()
 
# Define schema explicitly for the specific fields
schema = StructType([StructField(field, StringType(), nullable=True) for field in field_names])
 
# Create DataFrame with explicit schema
df = spark.createDataFrame(records, schema)

#display(df)

#Convert the fetched records into a DataFrame
df.createOrReplaceTempView("View_Account")

spark.sql("""
    CREATE OR REPLACE TABLE bronze.Account
    AS SELECT * FROM View_Account
""")

# COMMAND ----------

# %sql
# SELECT * FROM BRONZE.ACCOUNT;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
 
# # Define the specific fields to fetch
# fields_to_fetch = ["Id", "loan__Loan_Account__c", "loan__Interest_Accrued__c", "loan__Interest_Remaining__c","CreatedDate","CreatedById","LastModifiedById","LastModifiedDate"]

# Get all fields for the Account object using Metadata API
contact_fields = sfc.Contact.describe()["fields"]

 
# Construct the field list for the query
field_names = [field["name"] for field in contact_fields]
fields = ", ".join(field_names)
 
# Execute the query to fetch only the specified fields
query = f"SELECT {fields} FROM contact"
result = sfc.query_all(query)
records = result["records"]
 
# Create SparkSession
spark = SparkSession.builder.getOrCreate()
 
# Define schema explicitly for the specific fields
schema = StructType([StructField(field, StringType(), nullable=True) for field in field_names])
 
# Create DataFrame with explicit schema
df = spark.createDataFrame(records, schema)

#display(df)

#Convert the fetched records into a DataFrame
df.createOrReplaceTempView("View_Contact")

spark.sql("""
    CREATE OR REPLACE TABLE bronze.contact
    AS SELECT * FROM View_Contact
""")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
 


# Get all fields for the Account object using Metadata API
job_application_fields = sfc.Job_Application__c.describe()["fields"]

 
# Construct the field list for the query
field_names = [field["name"] for field in job_application_fields]
fields = ", ".join(field_names)
 
# Execute the query to fetch only the specified fields
query = f"SELECT {fields} FROM Job_Application__c"
result = sfc.query_all(query)
records = result["records"]

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Define schema explicitly for the specific fields
schema = StructType([StructField(field, StringType(), nullable=True) for field in field_names])

# Create DataFrame with explicit schema
df = spark.createDataFrame(records, schema)

#display(df)

#Convert the fetched records into a DataFrame
df.createOrReplaceTempView("View_Job_Application")

spark.sql("""
    CREATE OR REPLACE TABLE bronze.Job_Application
    AS SELECT * FROM View_Job_Application
""")

# COMMAND ----------


