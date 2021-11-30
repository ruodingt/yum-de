from argparse import ArgumentParser

import pytest
from jsonargparse import namespace_to_dict

from yumr.run import ReportingEngine, cli
import sys


# @pytest.fixture(scope='session')
# def cfg_test():
#     # sys.args = []
#     sys.argv = ([__file__, '--config', 'configs/test.yaml'])
#     _cfg = parse_cfg()
#     return _cfg


@pytest.fixture(scope='session')
def report_engine():
    sys.argv = ([__file__, '--config', 'configs/test/test_init.yaml', 'show_ddl'])
    reporting_engine = cli(run_subcommand=False)
    return reporting_engine



