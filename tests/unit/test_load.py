
def test_reporting_engine_init(report_engine):
    x = report_engine.spark.sql('show tables')
    tables = x.toPandas()['tableName'].values.tolist()
    assert tables == ['line_items', 'orders', 'venues']


def test_exec_sql_config_execution(report_engine):
    # todo: test the output dataframe has the expected content (matching exactly same content)
    pass


