# %python
# In utils/Silver_Table_Ingestion/ValueProcessor.py
class ValueProcessor:
    def __init__(self, values, spark):
        self.values = values
        self.spark = spark

    def process_value(self):
        for value in self.values:
            self.create_table(value)
        return f"Processed values: {', '.join(self.values)}"

    def create_table(self, user_input):
        if "Account" in user_input:
            table_name = f"SILVER.DIM_{user_input.upper()}"
            insert_table_sql = f"""
            INSERT INTO {table_name}(
            ACCOUNT_ID,
            ACCOUNT_NAME,
            ACCOUNT_TYPE,
            ACCOUNT_STATUS,
            ACCOUNT_CREATED_DATE,
            ACCOUNT_UPDATED_DATE)
            select
            acc.ACCOUNT_ID,
            acc.ACCOUNT_NAME,
            acc.ACCOUNT_TYPE,
            acc.ACCOUNT_STATUS,
            acc.ACCOUNT_CREATED_DATE,
            acc.ACCOUNT_UPDATED_DATE
            from bronze.account acc;
            """
            self.spark.sql(insert_table_sql)
            print(f"Data in the {table_name} inserted successfully!")
        elif "Contact" in user_input:
            # Handle other cases
            pass



# class ValueProcessor:
#     def __init__(self, values):
#         self.values = values

#     def create_table(self, user_input):
#         if "Application" in user_input:
#             table_name = f"SILVER.FACT_{user_input.upper()}"
#             insert_table_sql = f"""
#             INSERT INTO {table_name}(
#             APPLICATION_ID,
#             APPLICATION_NAME,
#             SFDC_CREATED_DATE,
#             SFDC_CREATED_BY,
#             SFDC_LAST_MODIFIED_DATE,
#             SFDC_LAST_MODIFIED_BY
#             )
#             Select
#             app.id,
#             app.name,
#             app.CreatedDate,
#             app.CreatedByid,
#             app.LastModifiedDate,
#             app.LastModifiedById
#             from bronze.job_application app;
#             """
#             spark.sql(insert_table_sql)
#             print(f"Data in the {table_name} inserted successfully!")

#         elif "Account" in user_input:
#             table_name = f"SILVER.DIM_{user_input.upper()}"
#             insert_table_sql = f"""
#             INSERT INTO {table_name}(
#             ACCOUNT_ID,
#             NAME,
#             ADDRESS,
#             INDUSTRY,
#             SFDC_CREATED_DATE,
#             SFDC_CREATED_BY,
#             SFDC_LAST_MODIFIED_DATE,
#             SFDC_LAST_MODIFIED_BY
#             )
#             Select
#             acc.id,
#             acc.name,
#             acc.billingStreet,
#             acc.industry,
#             acc.CreatedDate,
#             acc.CreatedByid,
#             acc.LastModifiedDate,
#             acc.LastModifiedById
#             from bronze.account acc;
#             """
#             spark.sql(insert_table_sql)
#             print(f"Data in the {table_name} inserted successfully!")

#         elif "Contact" in user_input:
#             table_name = f"SILVER.DIM_{user_input.upper()}"
#             insert_table_sql = f"""
#             INSERT INTO {table_name}(
#             CONTACT_ID,
#             NAME,
#             MAILING_STREET,
#             EMAIL,
#             ACCOUNT_ID,
#             ACCOUNT_NUMBER,
#             TYPE,
#             INDUSTRY,
#             ANNULARY_REVENUE,
#             ACTIVE,
#             SFDC_CREATED_DATE,
#             SFDC_CREATED_BY,
#             SFDC_LAST_MODIFIED_DATE,
#             SFDC_LAST_MODIFIED_BY
#             )
#             SELECT
#             CON.Id,
#             CON.Name,
#             CON.MAILINGSTREET,
#             CON.EMAIL,
#             CON.AccountId,
#             ACC.AccountNumber,
#             ACC.Type,
#             ACC.INDUSTRY,
#             ACC.AnnualRevenue,
#             ACC.Active__c,
#             CON.CreatedDate,
#             CON.CreatedById,
#             CON.LastModifiedDate,
#             CON.LastModifiedById
#             FROM bronze.contact CON LEFT JOIN bronze.account ACC ON CON.AccountId = ACC.Id
#             WHERE CON.ID NOT IN (SELECT CONTACT_ID FROM SILVER.dim_contact);
#             """
#             spark.sql(insert_table_sql)
#             print(f"Data in the {table_name} inserted successfully!")
#         else:
#             print("No valid table selected. Skipping table ingestion.")

#     def process_value(self):
#         for value in self.values:
#             self.create_table(value)
#         return f"Processed values: {', '.join(self.values)}"

