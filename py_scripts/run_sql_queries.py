from google.cloud import bigquery
from datetime import datetime

# Define the function that runs the query
def run_query_func(path):
    # Instantiate a BQ client
    client = bigquery.Client(project = 'logistics-data-staging-flat')

    # Read the SQL file
    f = open(path, 'r')
    sql_script = f.read()
    f.close()

    # Run the SQL script
    client.query(sql_script).result()

    # Print a success messaage
    print('The SQL script was executed successfully at {} \n'.format(datetime.now()))