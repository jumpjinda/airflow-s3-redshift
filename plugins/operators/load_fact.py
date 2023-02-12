from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator does load data to specified redshift table from staging_events or staging_songs by queries
    
    redshift_conn_id: Redshift connection in airflow
    table_name: Table where the data should be loaded
    insert_sql: queries data to loading to redshift table
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_conn_id="",
                table_name="",
                insert_sql="",
                append_data="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.insert_sql = insert_sql
        self.append_data = append_data

    def execute(self, context):
        self.log.info("LoadFactOperator is executing")

        redshift = PostgresHook(postgrest_conn_id=self.redshift_conn_id)

        if self.append_data == True:
            sql_statement = f'INSERT INTO {self.table_name} {self.insert_sql}'
            redshift.run(sql_statement)

            self.log.info(f"LoadFactOperator insert to {self.table_name} has completed")
        else:
            sql_statement = f'DELETE FROM {self.table_name}'
            redshift.run(sql_statement)

            sql_statement = f'INSERT INTO {self.table_name} {self.insert_sql}'
            redshift.run(sql_statement)

            self.log.info(f"LoadFactOperator insert to {self.table_name} has completed")