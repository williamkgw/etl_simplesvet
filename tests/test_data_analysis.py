import unittest

from pathlib import Path
import pandas as pd

def _test_output_xlsx_files(file_stems):
        DATASET_OUTPUT_PATH = Path("datasets/output")

        file_paths = [
                (DATASET_OUTPUT_PATH.joinpath(f"{file_stem}.xlsx"), DATASET_OUTPUT_PATH.joinpath(f"{file_stem}_1.xlsx"))
                for file_stem in file_stems
        ]

        for file, comparison in file_paths:
            dfs = pd.read_excel(file, sheet_name = None)
            dfs_comparison = pd.read_excel(comparison, sheet_name = None)

            assert dfs.keys() == dfs_comparison.keys()
            sheets = dfs.keys()

            for sheet in sheets:
                try:
                    pd.testing.assert_frame_equal(dfs[sheet], dfs_comparison[sheet])
                except Exception as e:
                    raise Exception(f"{file}:{sheet} with error {e}")


class TestDataAnalysis(unittest.TestCase):

    def test_sales(self):
        file_stems = ("missing_vendas_csv", "vendas_csv", "test_agg")
        _test_output_xlsx_files(file_stems)

    def test_clients(self):
        file_stems = ("clientes_csv", "test_agg_clientes")
        _test_output_xlsx_files(file_stems)

