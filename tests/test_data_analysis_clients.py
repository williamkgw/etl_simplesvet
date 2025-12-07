import unittest

from io import StringIO
from pathlib import Path
import pandas as pd

from etl_simplesvet.transformers.transform_pandas_data_analysis_clients import (
    agg_clientes_mapping
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


class TestDataAnalysisClients(unittest.TestCase):

    def test_clients(self):
        file_stems = ("clientes_csv", "test_agg_clientes")
        _test_output_xlsx_files(file_stems)

    def test_agg_clientes_mapping(clients_df):
        clients_mock_csv = """Inclusão,Origem,_grupo
            2023-01-18,fachada da loja,Fachada
            2023-01-27,google,Google
            2023-01-12,indicação de clientes,Indicação Clientes
            2023-01-07,fachada da loja,Fachada
            2023-01-02,fachada da loja,Fachada
            2023-01-16,fachada da loja,Fachada
            2023-01-25,fachada da loja,Fachada
            2023-01-24,fachada da loja,Fachada
            2023-01-16,fachada da loja,Fachada
            2023-01-10,fachada da loja,Fachada
            2023-02-27,fachada da loja,Fachada
            2023-02-13,indicação de clientes,Indicação Clientes
            2023-02-08,indicação de clientes,Indicação Clientes
            2023-02-09,fachada da loja,Fachada
            2023-02-14,google,Google
            2023-02-27,indicação de clientes,Indicação Clientes
            2023-02-10,fachada da loja,Fachada
            2023-02-07,indicação de clientes,Indicação Clientes
            2023-02-06,internet,Outros
            2023-02-06,indicação de parceiros,Indicação Parceiros
            2023-03-06,google,Google
            2023-03-06,google,Google
            2023-03-07,indicação de funcionários,Indicação Funcionarios
            2023-03-11,google,Google
            2023-03-01,indicação de parceiros,Indicação Parceiros
            2023-03-31,fachada da loja,Fachada
            2023-03-28,instagram/facebook,Facebook/Instagram
            2023-03-11,plano de saúde,Outros
            2023-03-14,indicação de clientes,Indicação Clientes
            2023-03-31,indicação de clientes,Indicação Clientes
            2023-04-08,google,Google
            2023-04-22,fachada da loja,Fachada
            2023-04-04,instagram/facebook,Facebook/Instagram
            2023-04-24,indicação de funcionários,Indicação Funcionarios
            2023-04-10,indicação de clientes,Indicação Clientes
            2023-04-01,google,Google
            2023-04-01,google,Google
            2023-04-25,google,Google
            2023-04-10,plano de saúde,Outros
            2023-04-14,fachada da loja,Fachada
        """
        df_clients_mock = pd.read_csv(StringIO(clients_mock_csv))
        df_clients_mock["Inclusão"] = pd.to_datetime(df_clients_mock["Inclusão"])
        agg = agg_clientes_mapping(df_clients_mock)
        agg_flattened = agg.unstack(level = -1).reset_index()

        agg_expected_flattened_csv = """level_0,_grupo,Inclusão,0
            Quantidade Totalizada Clientes,Facebook/Instagram,2023-01-31,0
            Quantidade Totalizada Clientes,Facebook/Instagram,2023-02-28,0
            Quantidade Totalizada Clientes,Facebook/Instagram,2023-03-31,1
            Quantidade Totalizada Clientes,Facebook/Instagram,2023-04-30,1
            Quantidade Totalizada Clientes,Fachada,2023-01-31,8
            Quantidade Totalizada Clientes,Fachada,2023-02-28,3
            Quantidade Totalizada Clientes,Fachada,2023-03-31,1
            Quantidade Totalizada Clientes,Fachada,2023-04-30,2
            Quantidade Totalizada Clientes,Google,2023-01-31,1
            Quantidade Totalizada Clientes,Google,2023-02-28,1
            Quantidade Totalizada Clientes,Google,2023-03-31,3
            Quantidade Totalizada Clientes,Google,2023-04-30,4
            Quantidade Totalizada Clientes,Indicação Clientes,2023-01-31,1
            Quantidade Totalizada Clientes,Indicação Clientes,2023-02-28,4
            Quantidade Totalizada Clientes,Indicação Clientes,2023-03-31,2
            Quantidade Totalizada Clientes,Indicação Clientes,2023-04-30,1
            Quantidade Totalizada Clientes,Indicação Funcionarios,2023-01-31,0
            Quantidade Totalizada Clientes,Indicação Funcionarios,2023-02-28,0
            Quantidade Totalizada Clientes,Indicação Funcionarios,2023-03-31,1
            Quantidade Totalizada Clientes,Indicação Funcionarios,2023-04-30,1
            Quantidade Totalizada Clientes,Indicação Parceiros,2023-01-31,0
            Quantidade Totalizada Clientes,Indicação Parceiros,2023-02-28,1
            Quantidade Totalizada Clientes,Indicação Parceiros,2023-03-31,1
            Quantidade Totalizada Clientes,Indicação Parceiros,2023-04-30,0
            Quantidade Totalizada Clientes,Outros,2023-01-31,0
            Quantidade Totalizada Clientes,Outros,2023-02-28,1
            Quantidade Totalizada Clientes,Outros,2023-03-31,1
            Quantidade Totalizada Clientes,Outros,2023-04-30,1
        """
        agg_expected_flattened = pd.read_csv(StringIO(agg_expected_flattened_csv))
        agg_expected_flattened = agg_expected_flattened.rename(columns={"0": 0})
        agg_expected_flattened["level_0"] = agg_expected_flattened["level_0"].str.strip()
        agg_expected_flattened["Inclusão"] = pd.to_datetime(agg_expected_flattened["Inclusão"])

        pd.testing.assert_frame_equal(agg_flattened, agg_expected_flattened)

#       Still need to work on:
#
#       clients_df = enrich_clients_df(clients_df, mapping_clients_df)
#       agg_clientes_total = agg_clients_total(clients_df)
#       agg_v_clientes = agg_vendas_clientes(sales_df)
