import unittest

from io import StringIO
from pathlib import Path
import pandas as pd

from etl_simplesvet.transformers.transform_pandas_data_analysis_clients import (
    enrich_clients_df,
    agg_vendas_clientes,
    agg_clientes_mapping,
    agg_clients_total
)


class TestDataAnalysisClients(unittest.TestCase):

    def test_enrich_clients(self):
        clients_mock_csv = """Origem
            _outros
            _outros
            _outros
            facebook
            facebook
            facebook
            fachada da loja
            fachada da loja
            fachada da loja
            google
            google
            google
            indicação de clientes
            indicação de clientes
            indicação de clientes
            indicação de funcionários
            indicação de funcionários
            indicação de funcionários
            indicação de parceiros
            indicação de parceiros
            indicação de parceiros
            instagram/facebook
            instagram/facebook
            instagram/facebook
            internet
            internet
            internet
            panfleto
            panfleto
            panfleto
            pet love
            pet love
            pet love
            plamev
            plamev
            plamev
            plano de saúde
            plano de saúde
            plano de saúde
            revista
            revista
            revista
            vital pet
            vital pet
            vital pet
        """

        df_clients_mock = pd.read_csv(StringIO(clients_mock_csv))
        df_clients_mock["Origem"] = df_clients_mock["Origem"].str.strip()

        mapping_clients_mock_csv = """Origem,Grupo
            internet,Outros
            fachada da loja,Fachada
            _outros,Outros
            indicação de clientes,Indicação Clientes
            plano de saúde,Outros
            google,Google
            indicação de parceiros,Indicação Parceiros
            indicação de funcionários,Indicação Funcionarios
            instagran,Facebook/Instagram
            panfleto,Campanha
            revista,Campanha
            facebook,Facebook/Instagram
            vital pet,Outros
            plamev,Outros
            instagram/facebook,Facebook/Instagram
            pet love,Outros
        """
        df_mapping_clients_mock = pd.read_csv(StringIO(mapping_clients_mock_csv))
        df_mapping_clients_mock["Origem"] = df_mapping_clients_mock["Origem"].str.strip()
        df_mapping_clients_mock = df_mapping_clients_mock.set_index("Origem")
        agg = enrich_clients_df(df_clients_mock, df_mapping_clients_mock )

        agg_expected_csv = """Origem,_grupo
            _outros,Outros
            _outros,Outros
            _outros,Outros
            facebook,Facebook/Instagram
            facebook,Facebook/Instagram
            facebook,Facebook/Instagram
            fachada da loja,Fachada
            fachada da loja,Fachada
            fachada da loja,Fachada
            google,Google
            google,Google
            google,Google
            indicação de clientes,Indicação Clientes
            indicação de clientes,Indicação Clientes
            indicação de clientes,Indicação Clientes
            indicação de funcionários,Indicação Funcionarios
            indicação de funcionários,Indicação Funcionarios
            indicação de funcionários,Indicação Funcionarios
            indicação de parceiros,Indicação Parceiros
            indicação de parceiros,Indicação Parceiros
            indicação de parceiros,Indicação Parceiros
            instagram/facebook,Facebook/Instagram
            instagram/facebook,Facebook/Instagram
            instagram/facebook,Facebook/Instagram
            internet,Outros
            internet,Outros
            internet,Outros
            panfleto,Campanha
            panfleto,Campanha
            panfleto,Campanha
            pet love,Outros
            pet love,Outros
            pet love,Outros
            plamev,Outros
            plamev,Outros
            plamev,Outros
            plano de saúde,Outros
            plano de saúde,Outros
            plano de saúde,Outros
            revista,Campanha
            revista,Campanha
            revista,Campanha
            vital pet,Outros
            vital pet,Outros
            vital pet,Outros
        """

        agg_expected = pd.read_csv(StringIO(agg_expected_csv))
        agg_expected["Origem"] = agg_expected["Origem"].str.strip()

        pd.testing.assert_frame_equal(agg, agg_expected)

    def test_agg_vendas_clientes(self):
        clients_mock_csv = """Data e hora,Cliente
            2023-01-04 17:36:00,Simone Brust
            2023-01-13 11:00:00,Regina Mendes
            2023-01-31 15:38:00,Juselma Correia
            2023-01-03 11:37:00,Karen Nunes
            2023-01-11 11:43:00,Lucas Moura
            2023-01-13 15:24:00,Keila Araujo
            2023-01-18 17:22:00,Maricelia Lopes
            2023-01-16 16:19:00,Laleska Freire
            2023-01-16 15:59:00,Flavia Albano
            2023-01-31 15:38:00,Juselma Correia
            2023-02-27 15:38:00,Rafaela Carvalho
            2023-02-13 09:16:00,Diana Sother
            2023-02-14 08:38:00,Ariomar Souza
            2023-02-07 11:16:00,Josenilton Santos
            2023-02-28 16:32:00,Jeanete Sobral
            2023-02-01 14:17:00,Isadora Souza
            2023-02-14 10:10:00,Rita Anjos
            2023-02-01 10:53:00,Maria Felice
            2023-02-14 10:47:00,Mayara Araújo
            2023-02-02 13:48:00,Isabelle Lucca
            2023-03-09 09:18:00,Carolina Alves
            2023-03-10 08:16:00,Grace Ferreira
            2023-03-10 09:32:00,Ana Fahel
            2023-03-08 08:58:00,Ana Cristina
            2023-03-13 10:05:00,Valdelice Rabelo
            2023-03-22 15:21:00,Keila Araujo
            2023-03-15 14:41:00,Ilca Duarte
            2023-03-14 08:37:00,Sonia Almeida
            2023-03-23 08:50:00,Ana Cristina
            2023-03-13 09:43:00,Fernando Ferraz
        """
        df_clients_mock = pd.read_csv(StringIO(clients_mock_csv))
        df_clients_mock["Data e hora"] = pd.to_datetime(df_clients_mock["Data e hora"])

        agg = agg_vendas_clientes(df_clients_mock)

        agg_expected_csv = """Data e hora,Quantidade Totalizada Clientes Ativos
            2023-01-31,0
            2023-02-28,9
            2023-03-31,19
        """

        agg_expected = pd.read_csv(StringIO(agg_expected_csv))
        agg_expected["Data e hora"] = pd.to_datetime(agg_expected["Data e hora"])
        agg_expected = agg_expected.set_index("Data e hora")
        agg_expected.index.freq = "1ME"
        agg_expected = agg_expected["Quantidade Totalizada Clientes Ativos"]

        pd.testing.assert_series_equal(agg, agg_expected)

    def test_agg_clientes_mapping(self):
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

    def test_agg_clientes_total(self):
        clients_mock_csv = """Inclusão,Origem
            2023-02-14,indicação de funcionários
            2023-01-09,google
            2023-03-04,fachada da loja
            2023-01-04,fachada da loja
            2023-01-09,fachada da loja
            2023-03-30,instagram/facebook
            2023-03-03,indicação de clientes
            2023-03-31,fachada da loja
            2023-03-23,indicação de clientes
            2023-02-14,plano de saúde
            2023-01-26,indicação de clientes
            2023-01-18,indicação de clientes
            2023-01-14,indicação de clientes
            2023-01-04,indicação de funcionários
            2023-03-28,instagram/facebook
            2023-01-31,indicação de funcionários
            2023-01-07,google
            2023-03-11,google
            2023-01-02,fachada da loja
            2023-02-14,google
            2023-01-16,fachada da loja
            2023-01-06,fachada da loja
            2023-01-25,indicação de clientes
            2023-01-18,indicação de clientes
            2023-01-20,fachada da loja
            2023-01-05,indicação de clientes
            2023-01-11,fachada da loja
            2023-02-06,internet
            2023-01-14,indicação de clientes
            2023-01-04,google
        """
        df_clients_mock = pd.read_csv(StringIO(clients_mock_csv))
        df_clients_mock["Inclusão"] = pd.to_datetime(df_clients_mock["Inclusão"])
        agg = agg_clients_total(df_clients_mock)

        agg_expected_csv = """Inclusão,Quantidade Totalizada Clientes
            2023-01-31,19
            2023-02-28,4
            2023-03-31,7
        """
        agg_expected = pd.read_csv(StringIO(agg_expected_csv))
        agg_expected["Inclusão"] = pd.to_datetime(agg_expected["Inclusão"])
        agg_expected = agg_expected.set_index("Inclusão")
        agg_expected.index.freq = "1ME"

        pd.testing.assert_frame_equal(agg, agg_expected)

