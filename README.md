# DWH_ACCELERATOR
This Accelerator model helps in the automatic ingestion of data from Salesforce and it also provides the UI for user input.

DATABRICKS MARKETPLACE PROVIDER IDEA-BY DATA WAREHOUSE TEAM 
					By: Suryamani, Swabarna and Jayanth 
 

OUR OBJECTIVE 

Automate the creation of a Databricks cluster for seamless data processing. 

Integrate Salesforce for secure data ingestion into Databricks. 

Ingest raw data from Salesforce into Databricks and store it in Bronze tables. 

Cleanse and transform raw data in the Silver tables for further analysis. 

Create structured Datamarts from the transformed data for analytics and reporting. 

Enable easy integration with business intelligence tools like Tableau or Power BI. 

Streamline the entire end-to-end data pipeline to accelerate data-driven decision-making. 

 

 

CREATE CLUSTER 

 

Cluster Management: Creating and managing clusters in Databricks is essential for setting up an analytics environment. 

Manual Setup Challenges: Manual cluster creation can be time-consuming and prone to errors. 

Automation Benefits: Automating cluster creation using code ensures consistency, efficiency, and scalability in provisioning resources. 

API Utilization: The Databricks REST API enables programmatic cluster creation with predefined configurations, reducing manual intervention. 

Scalability: Automated cluster creation allows for dynamic scaling based on workload requirements, optimizing resource allocation. 

Resource Optimization: Automated termination of inactive clusters helps optimize costs by avoiding unnecessary resource usage. 

Agility and Cost-Efficiency: Automating the process contributes to a more agile, cost-effective, and maintainable Databricks environment. 

CODE SNIPPETS: 

 

 

requests: Imports the requests library, which is used to make HTTP requests. 

json: Imports the json library to handle JSON data (for converting Python objects to JSON format and vice versa). 

 

 

Sets the Databricks API endpoint URL (/api/2.0/clusters/create), which is used to create a cluster in Databricks. 

 

 

 

 

Authorization: Sets the authorization header to use the Databricks personal access token (Bearer xxxxxxxxxxx), which authenticates the API request. 

Content-Type: Specifies that the request payload will be in JSON format. 

 

 

 

 

Prompts the user to input a name for the cluster and stores it in the cluster_name variable. 

 

 

 

num_workers: Sets the number of workers for the cluster to 4 (predefined value). 

node_type_id: Sets the type of the node (e.g., i3.xlarge, a specific instance type). 

spark_version: Defines the Spark version (e.g., 15.4.x-scala2.12). 

 

 

The payload is a Python dictionary that defines the configuration for the cluster. 

cluster_name: Uses the user input for the cluster name. 

spark_version: Uses the predefined Spark version. 

node_type_id: Uses the predefined node type. 

num_workers: Uses the predefined number of workers. 

autotermination_minutes: Sets the cluster to auto-terminate after 120 minutes of inactivity to save resources. 

 

 

Sends a POST request to the Databricks API to create the cluster, passing the payload (cluster configuration) as JSON data. 

json.dumps(payload): Converts the payload dictionary into a JSON string for transmission in the request body. 

 

 

Check for Success: If the API response status code is 200 (OK), it means the cluster was created successfully. 

response.json(): Parses the JSON response from the API and retrieves the cluster_id to display. 

Prints a success message and the cluster_id of the newly created cluster. 

 

 

Failure: If the status code is not 200, it indicates a failure in cluster creation. 

Prints a failure message along with the detailed error response from the API (e.g., error message or code). 

 

 

 

 

 

 

 

UPDATING THE CLUSTER 

 

Next we will see how we can enable the updating of an existing Databricks cluster by retrieving its current configuration and allowing the user to modify specific details such as the cluster name, number of workers, node type, and Spark version. 

It leverages the Databricks API to fetch the current cluster details, prompts the user for new values, and sends an API request to apply the updates. This allows users to modify cluster settings dynamically without needing to create a new cluster. 

 

CODE SNIPPETS: 

 

 

requests: Imports the requests library, which is used to make HTTP requests to the Databricks API. 

json: Imports the json library to handle JSON data, allowing conversion between Python objects and JSON format. 

 

 

 

databricks_url: Defines the Databricks API endpoint (/api/2.0/clusters/edit) used to update an existing cluster's configuration. 

 

Authorization: Sets the authorization header with the Personal Access Token (PAT) for authentication when making API requests. 

Content-Type: Specifies the content type of the request body, which is JSON. 

 

 

 
 

list_clusters_url: Defines the API endpoint (/api/2.0/clusters/list) to retrieve a list of all clusters in the Databricks workspace. 

 

 

 

Sends a GET request to the list_clusters_url to retrieve the list of clusters in the Databricks environment using the authentication headers. 

 

 

Parses the response content (which is in JSON format) and stores it in the clusters variable as a Python dictionary. 

 

 

Prompts the user to enter the cluster ID of the cluster they want to update and stores it in the cluster_id variable. 

 

 

get_cluster_info(): Defines a function that will call the iterate_cluster() function to fetch cluster details and print them. 

Initializes an empty list temp to store the cluster details. 

 

 

 

iterate_cluster(): Loops through the list of clusters (clusters["clusters"]) to find the cluster matching the provided cluster_id. 

If a match is found, it extracts the current cluster name, number of workers, node type, and Spark version. 

These extracted values are stored in temp_arr and returned by the function. 

Prints the cluster ID and name for debugging. 

 

 

 

Calls the get_cluster_info() function to retrieve and store the current configuration of the cluster in the arr list. 

 

 

Prompts the user to enter the new cluster name and stores it in the new_cluster_name variable. 

 

 

Sets the new values for the number of workers, node type, and Spark version by reusing the values retrieved earlier from the arr list. 

 

 

Creates a dictionary payload containing the cluster's updated configuration: 

cluster_id: The ID of the cluster to be updated. 

cluster_name: The new name entered by the user. 

num_workers: The existing number of workers (unchanged). 

node_type_id: The existing node type (unchanged). 

spark_version: The existing Spark version (unchanged). 

 

 

Sends a POST request to the Databricks API with the updated cluster configuration (payload). 

 

 

If the response status code is 200, it indicates the update was successful, and a success message is printed. 

 

 

If the response status code is not 200, it indicates failure, and a failure message is printed along with the error details from the API response. 

 

 

 

CREATION OF BRONZE TABLE 

 

 

Above snapshot indicates that 2 modules are being imported. 

Sparksession – The entry point for working with structured data in spark. 

• Dataframe and dataset API: Create and manipulate dataframes 

• SQL execution: To run SQL queries on data 

• Configuration management: Configure spark application properties 

• Resource coordination: Manage distributed cluster resources 

StructType – Represents the overall schema of dataframe. 

StructField – Define a single column in schema. 

StructType – Represents the data type of a column as a string. 

 

From the above snippet,one can understand that salesforce module is imported from simple_salesforce library. 

This library enables to authenticate with salesforce, query data and perform CRUD (Create, Read, Update, Delete) operations. 

 

OAuth credentials are part of the authentication process when connecting to salesforce. 

Consumer_key: A unique identifier for connected app in salesforce. It identifies the app that want to connect. 

Consumer_secret: A secret token associated with connected app. It’s used along with the consumer_key to authenticate safely. 

Salesforce login details are salesforce account specific details that one wants to get access through notebooks. 

Username: Salesforce username (email) 

Password: Salesforce password for specified username 

The above code is hardcoded credentials and has scope of improvement. 

 

From the above snippet, it can be inferred that sfc is an instance of the salesforce class from the simple_salesforce library. It establishes a connection to the salesforce API using provided credentials. 

Parameters involved here, 

Username –> salesforce username 

Password –> password for the username 

Security_token = None –> Default is set to None, normally the salesforce API requires security token when authenticating, in this case since OAuth creds are being used security token is set to none. 

Consumer_key -> The unique identifier of the connected app in salesforce. 

Consumer_secret -> The secret token associated with the connected app, used for secure OAuth authentication. 

Domain = ‘login’ -> Specifies salesforce login domain. 

 

 

Above code snippet retrieves metadata for fields in the account object using salesforce’s metadata API and [“fields”] retrieves a list of dictionaries, where each dictionary contains metadata about a field. 

Then a list is constructed out of field names using list comprehension to extract name of each field from the metadata. 

After that a SOQL query is executed to fetch all matching records. 

 

Now, spark session is initiated which provides an interface to work with dataframes and work with spark SQL.  

Then a schema is defined based on the field names to define the structure of data frame. 

Dataframe is created to prepare the data for further analysis using spark. 

Then temporary SQL view is created to allow dataframe be queried using spark SQL. 

 

 

Using spark SQL, account table is created from temporary SQL view 

 

 

In similar way, contact and job application table objects are created. 

			              SILVER LAYER 

 

Ingesting data into Silver tables involves cleaning and enriching raw data from Bronze tables by applying transformations such as filtering, removing duplicates, and joining external datasets. The resulting data is structured and optimized for analysis, making it query-ready. This process is often incremental, ensuring only new or changed data is processed. 

 

 

MULTISELECT WIDGET FOR SILVER TABLE SELECTION 

Widget Setup: A dropdown widget is used to let the user select one or more tables from a predefined list. 

User Selection: The tables selected by the user are captured and processed into a comma-separated string. 

Parameter Passing: This string is then passed as an argument to another notebook for further processing. 

Execution & Result: The second notebook is executed with the selected parameter, and the result is captured for review. 

 

 

 

 

 

 

CODE SNIPPETS: 
 

 

The dbutils.widgets.multiselect function creates a dropdown widget that allows users to select one or more options. 

The available options are "Select Table", "Account", "Contact", and "Application". 

dbutils.widgets.get("table_name") retrieves the selected value from the widget. 

 

 

 

 

 

 

   Retrieve User Selection: 

The selected table(s) from the widget are retrieved using dbutils.widgets.get("table_name"). 

The split(",") method splits the selection string into a list of selected tables. 

    Prepare Value for Passing: 

The list of selected tables is then re-joined into a comma-separated string using ",".join(selected_tables) to prepare the value to be passed to another notebook. 

    Call Another Notebook: 

dbutils.notebook.run() executes another notebook (receiving_parameter) with a timeout of 60 seconds. 

The value_to_pass is passed as an argument (with the key "value") to the target notebook. 

 

 

 

 SILVER TABLE CREATION 

 

 

 

Next we  are dynamically creating tables in the Silver layer of a data warehouse based on selected values (e.g., Account, Application, Contact) passed as a parameter to a piece of code in a notebook. It generates SQL commands to create the corresponding dimension or fact tables and processes the input values, returning a summary of the created tables. 

CODE SNIPPETS: 

 

 

 

 

 

      Class Definition: 

ValueProcessor: A class that processes a list of table names and creates corresponding tables in the Silver layer of a data warehouse. 

      __init__ Method: 

Initializes the class with a list of values (table names). 

      create_table Method: 

 

Based on the selected table name, it constructs and executes a SQL CREATE TABLE statement. 

Creates Account, Application, or Contact tables with various fields. 

 

       Account Table Creation: 

Creates a dimension table for Account with fields such as ACCOUNT_ID, NAME, ADDRESS, and INDUSTRY. 

       Application Table Creation: 

Creates a fact table for Application with fields like APPLICATION_ID, APPLICATION_NAME, and other related fields. 

       Contact Table Creation: 

Creates a dimension table for Contact, including fields like CONTACT_ID, NAME, EMAIL, and linking to Account. 

       process_value Method: 

Iterates through the list of selected tables, calling create_table to create each table. 

        Get Input Values: 

Retrieves the user-selected table names from the widget input and splits them into a list. 

         Processing the Tables: 

Creates an instance of ValueProcessor with the selected values and calls process_value to generate the tables. 

 

For example let’s look at one of the functions for table creation 

 

 

 

 

	 

 

 

 

 

 

 

SILVER TABLE INGESTION 

 

Next we are entering data into Silver layer tables (e.g., Application, Account, Contact) by processing selected table names. It generates and executes SQL INSERT statements to transfer data from the Bronze layer to the Silver layer, and returns a summary of the processed tables. 

 

Here the value is being fetched from the user input in the widget : 

 

 

CODE SNIPPETS: 

 

 

 

 

 

 

Class Definition: ValueProcessor class processes selected table names (e.g., Application, Account, Contact). 

create_table Method: 

    Inserts data into Application, Account, or Contact tables in the Silver layer from   corresponding Bronze tables. 

Data Insertion for Application: 

    Inserts APPLICATION_ID, APPLICATION_NAME from bronze.job_application. 

Data Insertion for Account: 

     Inserts ACCOUNT_ID, NAME, ADDRESS, INDUSTRY from bronze.account. 

Data Insertion for Contact: 

     Inserts various Contact fields joined with Account data from bronze.contact. 

process_value Method: 

     Iterates over selected tables and calls create_table for each. 

Retrieve User Input: 

     Gets selected table names from the widget input. 

Execute Processing: 

     Creates a ValueProcessor instance and processes the selected tables. 

Return Result: 

     Returns the processed table names using dbutils.notebook.exit(). 

 
 

For eaxmple let’s look at one of the functions where the ingestion is happening. 

 
 
