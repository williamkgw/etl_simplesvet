import unittest

from io import StringIO
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from etl_simplesvet.transformers.transform_pandas_data_analysis_sales import (
    get_inadimplencia_df,
    get_agg_grupo_df
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

    def test_agg_grupo(self):
        sales_mock_csv = """Data e hora,Quantidade,Bruto,__pilar,__grupo
            2023-01-19 11:04:00,1.0,55.00,Banho e Tosa,Banho
            2023-02-25 17:52:00,1.0,8.00,Banho e Tosa,Outros BT
            2023-03-24 09:25:00,1.0,30.00,Banho e Tosa,Tosa
            2023-02-06 13:05:00,2.0,20.00,Banho e Tosa,Transporte
            2023-02-27 14:03:00,1.0,400.00,Cirurgia,Cirurgia
            2023-02-27 14:07:00,1.0,380.00,Cirurgia,Procedimentos Cirurgico
            2023-03-24 15:13:00,1.0,70.01,Clínica,Consulta
            2023-01-20 10:26:00,1.0,100.00,Clínica,Procedimentos Clínico
            2023-01-04 09:59:00,1.0,80.00,Clínica,Vacina
            2023-02-21 10:29:00,1.0,180.00,Exames,Imagem
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])
        grouped_sales_mock = df_sales_mock.groupby([pd.Grouper(key = "Data e hora", freq = '1ME'), "__pilar", "__grupo"])

        agg_grupo_df = get_agg_grupo_df(grouped_sales_mock)
        agg_grupo_flattened_df = agg_grupo_df.unstack(level = -1).reset_index()

        agg_grupo_expected_csv = """level_0,__pilar,__grupo,Data e hora,0
            Faturamento Bruto,Banho e Tosa,Banho,2023-01-31,55.0
            Faturamento Bruto,Banho e Tosa,Banho,2023-02-28,0.0
            Faturamento Bruto,Banho e Tosa,Banho,2023-03-31,0.0
            Faturamento Bruto,Clínica,Procedimentos Clínico,2023-01-31,100.0
            Faturamento Bruto,Clínica,Procedimentos Clínico,2023-02-28,0.0
            Faturamento Bruto,Clínica,Procedimentos Clínico,2023-03-31,0.0
            Faturamento Bruto,Clínica,Vacina,2023-01-31,80.0
            Faturamento Bruto,Clínica,Vacina,2023-02-28,0.0
            Faturamento Bruto,Clínica,Vacina,2023-03-31,0.0
            Faturamento Bruto,Banho e Tosa,Outros BT,2023-01-31,0.0
            Faturamento Bruto,Banho e Tosa,Outros BT,2023-02-28,8.0
            Faturamento Bruto,Banho e Tosa,Outros BT,2023-03-31,0.0
            Faturamento Bruto,Banho e Tosa,Transporte,2023-01-31,0.0
            Faturamento Bruto,Banho e Tosa,Transporte,2023-02-28,20.0
            Faturamento Bruto,Banho e Tosa,Transporte,2023-03-31,0.0
            Faturamento Bruto,Cirurgia,Cirurgia,2023-01-31,0.0
            Faturamento Bruto,Cirurgia,Cirurgia,2023-02-28,400.0
            Faturamento Bruto,Cirurgia,Cirurgia,2023-03-31,0.0
            Faturamento Bruto,Cirurgia,Procedimentos Cirurgico,2023-01-31,0.0
            Faturamento Bruto,Cirurgia,Procedimentos Cirurgico,2023-02-28,380.0
            Faturamento Bruto,Cirurgia,Procedimentos Cirurgico,2023-03-31,0.0
            Faturamento Bruto,Exames,Imagem,2023-01-31,0.0
            Faturamento Bruto,Exames,Imagem,2023-02-28,180.0
            Faturamento Bruto,Exames,Imagem,2023-03-31,0.0
            Faturamento Bruto,Banho e Tosa,Tosa,2023-01-31,0.0
            Faturamento Bruto,Banho e Tosa,Tosa,2023-02-28,0.0
            Faturamento Bruto,Banho e Tosa,Tosa,2023-03-31,30.0
            Faturamento Bruto,Clínica,Consulta,2023-01-31,0.0
            Faturamento Bruto,Clínica,Consulta,2023-02-28,0.0
            Faturamento Bruto,Clínica,Consulta,2023-03-31,70.01
            Quantidade Totalizada,Banho e Tosa,Banho,2023-01-31,1.0
            Quantidade Totalizada,Banho e Tosa,Banho,2023-02-28,0.0
            Quantidade Totalizada,Banho e Tosa,Banho,2023-03-31,0.0
            Quantidade Totalizada,Clínica,Procedimentos Clínico,2023-01-31,1.0
            Quantidade Totalizada,Clínica,Procedimentos Clínico,2023-02-28,0.0
            Quantidade Totalizada,Clínica,Procedimentos Clínico,2023-03-31,0.0
            Quantidade Totalizada,Clínica,Vacina,2023-01-31,1.0
            Quantidade Totalizada,Clínica,Vacina,2023-02-28,0.0
            Quantidade Totalizada,Clínica,Vacina,2023-03-31,0.0
            Quantidade Totalizada,Banho e Tosa,Outros BT,2023-01-31,0.0
            Quantidade Totalizada,Banho e Tosa,Outros BT,2023-02-28,1.0
            Quantidade Totalizada,Banho e Tosa,Outros BT,2023-03-31,0.0
            Quantidade Totalizada,Banho e Tosa,Transporte,2023-01-31,0.0
            Quantidade Totalizada,Banho e Tosa,Transporte,2023-02-28,2.0
            Quantidade Totalizada,Banho e Tosa,Transporte,2023-03-31,0.0
            Quantidade Totalizada,Cirurgia,Cirurgia,2023-01-31,0.0
            Quantidade Totalizada,Cirurgia,Cirurgia,2023-02-28,1.0
            Quantidade Totalizada,Cirurgia,Cirurgia,2023-03-31,0.0
            Quantidade Totalizada,Cirurgia,Procedimentos Cirurgico,2023-01-31,0.0
            Quantidade Totalizada,Cirurgia,Procedimentos Cirurgico,2023-02-28,1.0
            Quantidade Totalizada,Cirurgia,Procedimentos Cirurgico,2023-03-31,0.0
            Quantidade Totalizada,Exames,Imagem,2023-01-31,0.0
            Quantidade Totalizada,Exames,Imagem,2023-02-28,1.0
            Quantidade Totalizada,Exames,Imagem,2023-03-31,0.0
            Quantidade Totalizada,Banho e Tosa,Tosa,2023-01-31,0.0
            Quantidade Totalizada,Banho e Tosa,Tosa,2023-02-28,0.0
            Quantidade Totalizada,Banho e Tosa,Tosa,2023-03-31,1.0
            Quantidade Totalizada,Clínica,Consulta,2023-01-31,0.0
            Quantidade Totalizada,Clínica,Consulta,2023-02-28,0.0
            Quantidade Totalizada,Clínica,Consulta,2023-03-31,1.0
            Preço Médio,Banho e Tosa,Banho,2023-01-31,55.0
            Preço Médio,Banho e Tosa,Banho,2023-02-28,0.0
            Preço Médio,Banho e Tosa,Banho,2023-03-31,0.0
            Preço Médio,Clínica,Procedimentos Clínico,2023-01-31,100.0
            Preço Médio,Clínica,Procedimentos Clínico,2023-02-28,0.0
            Preço Médio,Clínica,Procedimentos Clínico,2023-03-31,0.0
            Preço Médio,Clínica,Vacina,2023-01-31,80.0
            Preço Médio,Clínica,Vacina,2023-02-28,0.0
            Preço Médio,Clínica,Vacina,2023-03-31,0.0
            Preço Médio,Banho e Tosa,Outros BT,2023-01-31,0.0
            Preço Médio,Banho e Tosa,Outros BT,2023-02-28,8.0
            Preço Médio,Banho e Tosa,Outros BT,2023-03-31,0.0
            Preço Médio,Banho e Tosa,Transporte,2023-01-31,0.0
            Preço Médio,Banho e Tosa,Transporte,2023-02-28,10.0
            Preço Médio,Banho e Tosa,Transporte,2023-03-31,0.0
            Preço Médio,Cirurgia,Cirurgia,2023-01-31,0.0
            Preço Médio,Cirurgia,Cirurgia,2023-02-28,400.0
            Preço Médio,Cirurgia,Cirurgia,2023-03-31,0.0
            Preço Médio,Cirurgia,Procedimentos Cirurgico,2023-01-31,0.0
            Preço Médio,Cirurgia,Procedimentos Cirurgico,2023-02-28,380.0
            Preço Médio,Cirurgia,Procedimentos Cirurgico,2023-03-31,0.0
            Preço Médio,Exames,Imagem,2023-01-31,0.0
            Preço Médio,Exames,Imagem,2023-02-28,180.0
            Preço Médio,Exames,Imagem,2023-03-31,0.0
            Preço Médio,Banho e Tosa,Tosa,2023-01-31,0.0
            Preço Médio,Banho e Tosa,Tosa,2023-02-28,0.0
            Preço Médio,Banho e Tosa,Tosa,2023-03-31,30.0
            Preço Médio,Clínica,Consulta,2023-01-31,0.0
            Preço Médio,Clínica,Consulta,2023-02-28,0.0
            Preço Médio,Clínica,Consulta,2023-03-31,70.01
        """
        agg_grupo_flattened_expected_df = pd.read_csv(StringIO(agg_grupo_expected_csv))
        agg_grupo_flattened_expected_df = agg_grupo_flattened_expected_df.rename(columns={'0': 0})
        agg_grupo_flattened_expected_df["level_0"] = agg_grupo_flattened_expected_df["level_0"].str.strip()
        agg_grupo_flattened_expected_df["Data e hora"] = pd.to_datetime(agg_grupo_flattened_expected_df["Data e hora"])
        pd.testing.assert_frame_equal(agg_grupo_flattened_df, agg_grupo_flattened_expected_df)

    def test_inadimplencia(self):
        sales_mock_csv = """Data e hora,Status da venda,Bruto
            2024-05-12 10:27:00,Baixado,400.0
            2024-05-12 10:27:00,Baixado,400.0
            2024-05-12 10:27:00,Baixado,600.0
            2024-01-04 18:21:00,Baixado,312.0
            2024-01-04 18:21:00,Baixa parcial,276.0
            2024-09-17 11:33:00,Baixa parcial,36.0
            2024-10-24 13:50:00,Aberto,63.0
            2024-11-13 17:44:00,Baixa parcial,8.0
            2024-12-21 15:04:00,Aberto,36.0
            2025-01-14 08:35:00,Em atendimento,220.0
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])
        end_date = datetime(2025, 2, 1, 0, 0)
        s_inadimpl = get_inadimplencia_df(df_sales_mock, end_date)

        inadimpl_expected_csv ="""Data e hora,Inadimplencia do Faturamento Bruto
            2024-01-31 00:00:00,
            2024-02-29 00:00:00,
            2024-03-31 00:00:00,
            2024-04-30 00:00:00,
            2024-05-31 00:00:00,
            2024-06-30 00:00:00,
            2024-07-31 00:00:00,
            2024-08-31 00:00:00,
            2024-09-30 00:00:00,
            2024-10-31 00:00:00,
            2024-11-30 00:00:00,
            2024-12-31 00:00:00,419.0
        """
        s_inadimpl_expected = pd.read_csv(StringIO(inadimpl_expected_csv))
        s_inadimpl_expected["Data e hora"] = pd.to_datetime(s_inadimpl_expected["Data e hora"])
        s_inadimpl_expected = s_inadimpl_expected.set_index("Data e hora")["Inadimplencia do Faturamento Bruto"]
        s_inadimpl_expected.index = s_inadimpl_expected.index.to_period("M").to_timestamp("M")

        pd.testing.assert_series_equal(s_inadimpl, s_inadimpl_expected)

        #    STILL NEED TO DO THAT
        #
        #    "agg_pilar_df": agg_pilar_df.loc[agg_pilar_df.index[-6:]],
        #    "agg_categoria_df": agg_categoria_df.loc[agg_categoria_df.index[-6:]],
        #    "agg_tempo_df": agg_tempo_df.loc[agg_tempo_df.index[-6:]],
        #    "agg_exception_df": exception_df.loc[exception_df.index[-6:]],
        #    "unique_mapping_df": unique_mapping_df,
        #    "vendas_missing_df": sales_missing_df,
        #    "sales_df": sales_df,

