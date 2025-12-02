import unittest

from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from etl_simplesvet.transformers.transform_pandas_data_analysis_sales import (
    get_inadimplencia_df
)

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



class TestDataAnalysisSales(unittest.TestCase):

    def test_sales(self):
        file_stems = ("missing_vendas_csv", "vendas_csv", "test_agg")
        _test_output_xlsx_files(file_stems)

    def test_inadimplencia(self):
        mock_data = {
                'Data e hora': {
                    0: pd.Timestamp('2024-05-12 10:27:00'),
                    1: pd.Timestamp('2024-05-12 10:27:00'),
                    2: pd.Timestamp('2024-05-12 10:27:00'),
                    3: pd.Timestamp('2024-01-04 18:21:00'),
                    4: pd.Timestamp('2024-01-04 18:21:00'),
                    5: pd.Timestamp('2024-09-17 11:33:00'),
                    6: pd.Timestamp('2024-10-24 13:50:00'),
                    7: pd.Timestamp('2024-11-13 17:44:00'),
                    8: pd.Timestamp('2024-12-21 15:04:00'),
                    9: pd.Timestamp('2025-01-14 08:35:00')
                },
                'Status da venda': {
                    0: 'Baixado',
                    1: 'Baixado',
                    2: 'Baixado',
                    3: 'Baixado',
                    4: 'Baixa parcial',
                    5: 'Baixa parcial',
                    6: 'Aberto',
                    7: 'Baixa parcial',
                    8: 'Aberto',
                    9: 'Em atendimento'
                },
                'Bruto': {
                    0: 400.0,
                    1: 400.0,
                    2: 600.0,
                    3: 312.0,
                    4: 276.0,
                    5: 36.0,
                    6: 63.0,
                    7: 8.0,
                    8: 36.0,
                    9: 220.0
                  }
                }
        df_sales_mock = pd.DataFrame(mock_data)
        end_date = datetime(2025, 2, 1, 0, 0)
        s_inadimpl = get_inadimplencia_df(df_sales_mock, end_date)

        s_inadimpl_expected = pd.Series({
                pd.Timestamp('2024-01-31 00:00:00'): np.nan,
                pd.Timestamp('2024-02-29 00:00:00'): np.nan,
                pd.Timestamp('2024-03-31 00:00:00'): np.nan,
                pd.Timestamp('2024-04-30 00:00:00'): np.nan,
                pd.Timestamp('2024-05-31 00:00:00'): np.nan,
                pd.Timestamp('2024-06-30 00:00:00'): np.nan,
                pd.Timestamp('2024-07-31 00:00:00'): np.nan,
                pd.Timestamp('2024-08-31 00:00:00'): np.nan,
                pd.Timestamp('2024-09-30 00:00:00'): np.nan,
                pd.Timestamp('2024-10-31 00:00:00'): np.nan,
                pd.Timestamp('2024-11-30 00:00:00'): np.nan,
                pd.Timestamp('2024-12-31 00:00:00'): 419.0
            },
            name = 'Inadimplencia do Faturamento Bruto')
        s_inadimpl_expected.index.name = 'Data e hora'
        s_inadimpl_expected.index = s_inadimpl_expected.index.to_period("M").to_timestamp("M")

        pd.testing.assert_series_equal(s_inadimpl, s_inadimpl_expected)

        #    STILL NEED TO DO THAT
        #
        #    "agg_grupo_df": agg_grupo_df.loc[agg_grupo_df.index[-6:]],
        #    "agg_pilar_df": agg_pilar_df.loc[agg_pilar_df.index[-6:]],
        #    "agg_categoria_df": agg_categoria_df.loc[agg_categoria_df.index[-6:]],
        #    "agg_tempo_df": agg_tempo_df.loc[agg_tempo_df.index[-6:]],
        #    "agg_exception_df": exception_df.loc[exception_df.index[-6:]],
        #    "unique_mapping_df": unique_mapping_df,
        #    "vendas_missing_df": sales_missing_df,
        #    "sales_df": sales_df,

class TestDataAnalysisClients(unittest.TestCase):

    def test_clients(self):
        file_stems = ("clientes_csv", "test_agg_clientes")
        _test_output_xlsx_files(file_stems)

