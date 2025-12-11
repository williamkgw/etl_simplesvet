import unittest
import pandas as pd


class TestExport(unittest.TestCase):

    def test_med(self):
        export = pd.read_excel("datasets/output/out_import_1.xlsx")
        export_expected = pd.read_excel("datasets/output/out_import.xlsx")
        pd.testing.assert_frame_equal(export, export_expected)

