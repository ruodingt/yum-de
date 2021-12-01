import inspect
import os.path
from datetime import datetime

from jsonargparse import namespace_to_dict, CLI
from pyspark.sql import SparkSession


class ReportingEngine:
    def __init__(self, master: str = "local[1]", app_name: str = "reporting.yum", load_configs: list = []):
        self.spark = SparkSession. \
            builder.master(master=master). \
            appName(app_name). \
            getOrCreate()

        self.load_configs = load_configs
        self._load_all_json()

    def _load_all_json(self):
        for load_conf in self.load_configs:
            self._load_json(src_json=load_conf['src'], dst_table=load_conf['dst'])

    def _load_json(self, src_json: str, dst_table: str):
        df = self.spark.read.json(src_json)
        df.registerTempTable(dst_table)

    @property
    def tables(self):
        x = self.spark.sql('show tables')
        tables = x.toPandas()['tableName'].values.tolist()
        return tables

    def show_ddl(self):
        for t in self.tables:
            df = self.spark.sql(f'SELECT * FROM {t} LIMIT 10')
            schema_json = df.schema.json()
            ddl = self.spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema_json).toDDL()
            print(f"[{t}]: {ddl}")

    def show_db(self):
        self.spark.sql('show tables').show()

    def exec(self, sql: str = '', output: str = "data/output/report_most_popular_item"):
        """
        execution example:
            python3 yumr/run.py --config configs/test/test_init.yaml exec --config configs/test/test_sql.yaml

        :param sql: sql command to executed
        :param output:
        :return:
        """
        print('executing sql:\n', sql)
        results = self.spark.sql(
            f"""{sql}""")
        results.show()
        output_dir = os.path.join(output, datetime.now().isoformat())
        results.write.json(output_dir)
        print(f'results output to [{output_dir}]')
        return results


def exec_report_engine(cfg):
    reporting_engine = ReportingEngine(**namespace_to_dict(cfg.reporting_engine.init))
    reporting_engine.show_db()
    reporting_engine.show_ddl()
    # reporting_engine.exec(**namespace_to_dict(cfg.reporting_engine.exec))
    return


def cli(run_subcommand=True):
    parser = CLI(ReportingEngine, return_parser=not run_subcommand)
    if not run_subcommand:
        init_args = list(inspect.signature(ReportingEngine).parameters)
        cfg = namespace_to_dict(parser.parse_args())
        return ReportingEngine(**{k: v for k, v in cfg.items() if k in init_args})


if __name__ == '__main__':

    # install the package and dependecies
    # pip3 install -e .
    # The command will init ReportingEngine and execute the sql command specified in configs/test/test_sql.yaml
    # python3 yumr/run.py --config configs/test/test_init.yaml exec --config configs/test/test_sql.yaml
    cli(run_subcommand=True)
