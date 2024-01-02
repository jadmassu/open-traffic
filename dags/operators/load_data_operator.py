from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
import os
class LoadDataOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id, source_path, target_table, *args, **kwargs):
        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_path = source_path
        self.target_table = target_table

    

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Get a list of all files in the specified folder
        files = os.listdir(self.source_path)

        for file_name in files:
            # Construct the full path of each file
            file_path = os.path.join(self.source_path, file_name)

            if file_name.endswith('.csv'):
                with open(file_path, 'r') as file:
                    # Read the lines from the file
                    lines = file.readlines()

                    # Custom parsing logic based on your data file format
                    # Parse the lines and extract the data fields for each row
                    data = []
                    for line in lines:
                        # Custom logic to extract the fields from each line
                        # and prepare the data for insertion
                        fields = line.strip().split(',')  # Example: using comma as a delimiter
                        data.append(fields)

                    # Perform the data loading into the target table
                    for row in data:
                        # Construct the INSERT query based on the target table structure
                        insert_query = f"INSERT INTO {self.target_table} VALUES ({','.join(row)});"
                        pg_hook.run(insert_query)