from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_conn_id="",
                dq_checks=[],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('DataQualityOperator is executing')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        error_count = 0
        sql_fails = []

        for check in self.dq_checks:
            check_sql = check.get('check_sql')
            expected_result = check.get('expected_result')

            try:
                self.log.info(f"Running query {check_sql}")
                result = redshift.get_records(check_sql)
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")

            if expected_result != result[0]:
                error_count += 1
                sql_fails.append(check_sql)

        if error_count > 0:
            self.log.info(f"Tests failed: {error_count} errors")
            for fail in sql_fails:
                self.log.info(f"Sql failed is: {fail}")
        else:
            self.log.info("All data quality checks passed")