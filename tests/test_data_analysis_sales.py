import unittest

from io import StringIO
import pandas as pd
import numpy as np
from datetime import datetime

from etl_simplesvet.transformers.transform_pandas_data_analysis_sales import (
    enrich_sales_df,
    get_agg_grupo_df,
    get_agg_pilar_df,
    get_agg_categoria_df,
    get_agg_tempo_df,
    get_inadimplencia_df,
    get_exception_df
)


class TestDataAnalysisSales(unittest.TestCase):

    def test_enrich_sales(self):
        sales_mock_csv = """Data e hora,Venda,Código,Produto/serviço
            2023-09-16 11:42:00,52190,6053.0,sedação até 5 a 10kg
            2025-04-07 14:24:00,63730,6662.0,internamento dia até 10kg
            2024-11-09 10:34:00,61593,4539.0,"sorologia- hemoparasitas quantitativo-pcr (ehrlichia spp, babesia spp, anaplasma spp) cl"
            2023-07-03 12:42:00,52113,2239.0,traumeel por ml
            2023-09-22 09:49:00,53837,6150.0,sedação até 5 a 10kg
            2023-09-28 18:06:00,53962,4993.0,teste accuvet erliquiose
            2024-01-31 10:52:00,56559,5835.0,vitalpet contagem de reticulócitos cod 1607
            2024-05-08 12:40:00,58335,5648.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais)
            2024-10-01 08:32:00,60908,2152.0,internamento dia até 10kg
            2023-08-03 16:39:00,52771,6059.0,dedeira un
            2023-08-19 12:17:00,53118,6107.0,dedeira un
            2023-08-19 13:32:00,53125,6106.0,bezzie risoto frango e vegetais 250gr
            2023-08-23 17:25:00,53191,3918.0,dedeira un
            2023-08-29 12:01:00,53309,5981.0,bezzie risoto frango e vegetais 250gr
            2023-08-29 12:28:00,53313,5335.0,bezzie risoto frango e vegetais 250gr
            2023-09-11 10:49:00,53571,799.0,bezzie risoto frango e vegetais 250gr
            2023-09-14 14:57:00,53658,4778.0,bezzie risoto frango e vegetais 250gr
            2023-09-23 10:01:00,53861,5978.0,bezzie risoto frango e vegetais 250gr
            2023-09-30 08:27:00,53998,5830.0,bezzie risoto frango e vegetais 250gr
            2023-10-05 18:28:00,54114,6148.0,bezzie risoto frango e vegetais 250gr
            2023-12-18 15:51:00,55619,3049.0,bezzie risoto frango e vegetais 250gr
            2024-01-02 12:00:00,55909,5747.0,bolsa tricoline passeio (fofuchos pet)
            2024-02-27 15:42:00,57021,3763.0,guia para cachorros jacquard gibson pp
            2024-03-09 13:30:00,57285,0.0,dedeira un
            2024-03-11 09:10:00,57297,0.0,dedeira un
            2024-07-09 16:03:00,59518,6438.0,dedeira un
            2024-08-08 15:13:00,60040,2354.0,peitoral para cachorros h nox lumen p
            2024-09-06 09:24:00,60526,6464.0,brinq cat guizo penas bulet bom amigo
            2024-09-12 16:22:00,60623,6073.0,mord. pelucia pop bicho lobo
            2024-09-21 12:22:00,60771,3624.0,mord pelucia macaco gd marrom kelev
            2024-10-10 09:16:00,61090,0.0,allequa 10ml
            2024-12-13 15:31:00,62069,4458.0,mord. pelucia pop bicho lobo
            2025-02-01 10:52:00,62808,2152.0,mord. pelucia pop bicho lobo
            2025-04-09 14:46:00,63779,5491.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais)
            2023-02-24 15:49:00,49520,5237.0,dedeira un
            2023-05-19 14:51:00,51203,5745.0,dedeira un
            2023-05-26 12:25:00,51333,2152.0,bezzie risoto frango e vegetais 250gr
            2023-06-19 14:14:00,51820,3049.0,bezzie risoto frango e vegetais 250gr
            2023-07-07 17:20:00,52221,5382.0,dedeira un
            2023-07-19 11:13:00,52431,3049.0,bezzie risoto frango e vegetais 250gr
            2023-07-19 11:13:00,52431,3049.0,bezzie risoto frango e vegetais 250gr
            2023-06-30 18:10:00,52084,2239.0,traumeel por ml
            2023-11-09 14:50:00,54817,2429.0,teste accuvet erliquiose
            2024-05-08 12:17:00,58329,5648.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais)
            2024-05-09 12:02:00,58410,5648.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais)
            2024-09-12 14:38:00,60616,6155.0,internamento dia até 10kg
            2024-11-08 13:10:00,61579,3458.0,internamento dia até 10kg
            2023-01-18 18:18:00,48750,5842.0,bezzie risoto frango e vegetais 250gr
            2023-01-21 11:32:00,48843,2152.0,bezzie risoto frango e vegetais 250gr
            2023-06-02 08:31:00,51477,5858.0,bezzie risoto frango e vegetais 250gr
            2023-01-11 13:57:00,48566,5825.0,dedeira un
            2023-01-13 17:45:00,48630,5827.0,dedeira un
            2023-01-21 12:17:00,48845,3049.0,bezzie risoto frango e vegetais 250gr
            2023-02-03 12:23:00,49125,3241.0,bezzie risoto frango e vegetais 250gr
            2023-03-15 09:18:00,49916,2152.0,bezzie risoto frango e vegetais 250gr
            2023-03-18 13:54:00,50015,5903.0,bezzie risoto frango e vegetais 250gr
            2023-03-23 08:50:00,50075,2152.0,bezzie risoto frango e vegetais 250gr
            2023-03-23 08:50:00,50075,2152.0,bezzie risoto frango e vegetais 250gr
            2023-04-15 10:07:00,50541,5465.0,allequa 10ml
            2023-05-04 10:57:00,50882,3049.0,bezzie risoto frango e vegetais 250gr
            2023-06-06 09:53:00,51548,6023.0,bezzie risoto frango e vegetais 250gr
            2023-06-30 12:36:00,52062,3049.0,bezzie risoto frango e vegetais 250gr
            2023-07-04 08:29:00,52121,5069.0,bezzie risoto frango e vegetais 250gr
            2023-07-06 11:56:00,52180,2152.0,dedeira un
            2023-07-07 12:12:00,52206,0.0,dedeira un
            2023-07-11 10:39:00,52267,3049.0,bezzie risoto frango e vegetais 250gr
            2023-07-20 08:52:00,52442,5069.0,bezzie risoto frango e vegetais 250gr
            2023-07-21 16:57:00,52506,0.0,dedeira un
            2023-07-29 08:59:00,52659,6081.0,dedeira un
            2023-09-14 13:37:00,53656,3049.0,bezzie risoto frango e vegetais 250gr
            2023-12-06 09:09:00,55341,2152.0,retangular imperial gg estampado
            2023-12-06 09:09:00,55341,2152.0,retangular imperial gg estampado
            2024-01-22 17:48:00,56368,0.0,"feline satiety 1,5 kg - un"
            2024-02-19 09:26:00,56853,5186.0,fitoclean 250ml
            2024-03-06 07:59:00,57186,6327.0,bolsa tricoline passeio (fofuchos pet)
            2024-11-08 08:40:00,61563,2896.0,brinq cat guizo penas bulet bom amigo
            2023-01-20 16:21:00,48814,5853.0,bezzie risoto frango e vegetais 250gr
            2023-05-06 16:26:00,50950,0.0,dedeira un
            2023-10-11 12:14:00,54227,0.0,bezzie risoto frango e vegetais 250gr
            2023-12-06 15:59:00,55359,4267.0,hydra groomers colonia forever gold 12
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])

        mapping_sales_mock_csv = '''Produto/serviço,Categoria,Pilar,Grupo
            internamento dia até 10kg,Clí+,Internação,Diária
            "feline satiety 1,5 kg - un",B&T+P&S,PetShop,Alimentos
            traumeel por ml,Clí+,Clínica,Procedimentos Clínico
            bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos
            teste accuvet erliquiose,B&T+P&S,PetShop,Farmácia
            fitoclean 250ml,B&T+P&S,PetShop,Farmácia
            dedeira un,B&T+P&S,PetShop,Acessórios
            sedação até 5 a 10kg,Clí+,Cirurgia,Procedimentos Cirurgico
            vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais),Clí+,Internação,Diária
            bolsa tricoline passeio (fofuchos pet),B&T+P&S,PetShop,Acessórios
            vitalpet contagem de reticulócitos cod 1607,Clí+,Exames,Laboratório
            guia para cachorros jacquard gibson pp,B&T+P&S,PetShop,Acessórios
            mord pelucia macaco gd marrom kelev,B&T+P&S,PetShop,Acessórios
            mord. pelucia pop bicho lobo,B&T+P&S,PetShop,Acessórios
            hydra groomers colonia forever gold 12,B&T+P&S,PetShop,Acessórios
            retangular imperial gg estampado,B&T+P&S,PetShop,Farmácia
            allequa 10ml,B&T+P&S,PetShop,Farmácia
            "sorologia- hemoparasitas quantitativo-pcr (ehrlichia spp, babesia spp, anaplasma spp) cl",Clí+,Exames,Laboratório
            peitoral para cachorros h nox lumen p,B&T+P&S,PetShop,Acessórios
            brinq cat guizo penas bulet bom amigo,B&T+P&S,PetShop,Acessórios
        '''
        df_mapping_sales_mock = pd.read_csv(StringIO(mapping_sales_mock_csv), skipinitialspace = True)
        df_mapping_sales_mock = df_mapping_sales_mock.set_index("Produto/serviço")

        df_sales_enrich = enrich_sales_df(df_sales_mock, df_mapping_sales_mock)

        sales_enrich_expected_csv = """Data e hora,Venda,Código,Produto/serviço,__categoria,__pilar,__grupo,__ano,__mes,__ticket,__ticket_por_pilar,__clientes_ativos,__clientes_ativo_por_pilar
            2023-09-16 11:42:00,52190,6053.0,sedação até 5 a 10kg,Clí+,Cirurgia,Procedimentos Cirurgico,2023,9,1.0,1.0,1.0,1.0
            2025-04-07 14:24:00,63730,6662.0,internamento dia até 10kg,Clí+,Internação,Diária,2025,4,1.0,1.0,1.0,1.0
            2024-11-09 10:34:00,61593,4539.0,"sorologia- hemoparasitas quantitativo-pcr (ehrlichia spp, babesia spp, anaplasma spp) cl",Clí+,Exames,Laboratório,2024,11,1.0,1.0,1.0,1.0
            2023-07-03 12:42:00,52113,2239.0,traumeel por ml,Clí+,Clínica,Procedimentos Clínico,2023,7,1.0,1.0,1.0,1.0
            2023-09-22 09:49:00,53837,6150.0,sedação até 5 a 10kg,Clí+,Cirurgia,Procedimentos Cirurgico,2023,9,1.0,1.0,1.0,1.0
            2023-09-28 18:06:00,53962,4993.0,teste accuvet erliquiose,B&T+P&S,PetShop,Farmácia,2023,9,1.0,1.0,1.0,1.0
            2024-01-31 10:52:00,56559,5835.0,vitalpet contagem de reticulócitos cod 1607,Clí+,Exames,Laboratório,2024,1,1.0,1.0,1.0,1.0
            2024-05-08 12:40:00,58335,5648.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais),Clí+,Internação,Diária,2024,5,1.0,1.0,0.3333333333333333,0.3333333333333333
            2024-10-01 08:32:00,60908,2152.0,internamento dia até 10kg,Clí+,Internação,Diária,2024,10,1.0,1.0,1.0,1.0
            2023-08-03 16:39:00,52771,6059.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,8,1.0,1.0,1.0,1.0
            2023-08-19 12:17:00,53118,6107.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,8,1.0,1.0,1.0,1.0
            2023-08-19 13:32:00,53125,6106.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,8,1.0,1.0,1.0,1.0
            2023-08-23 17:25:00,53191,3918.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,8,1.0,1.0,1.0,1.0
            2023-08-29 12:01:00,53309,5981.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,8,1.0,1.0,1.0,1.0
            2023-08-29 12:28:00,53313,5335.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,8,1.0,1.0,1.0,1.0
            2023-09-11 10:49:00,53571,799.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,9,1.0,1.0,1.0,1.0
            2023-09-14 14:57:00,53658,4778.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,9,1.0,1.0,1.0,1.0
            2023-09-23 10:01:00,53861,5978.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,9,1.0,1.0,1.0,1.0
            2023-09-30 08:27:00,53998,5830.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,9,1.0,1.0,1.0,1.0
            2023-10-05 18:28:00,54114,6148.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,10,1.0,1.0,1.0,1.0
            2023-12-18 15:51:00,55619,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,12,1.0,1.0,1.0,1.0
            2024-01-02 12:00:00,55909,5747.0,bolsa tricoline passeio (fofuchos pet),B&T+P&S,PetShop,Acessórios,2024,1,1.0,1.0,1.0,1.0
            2024-02-27 15:42:00,57021,3763.0,guia para cachorros jacquard gibson pp,B&T+P&S,PetShop,Acessórios,2024,2,1.0,1.0,1.0,1.0
            2024-03-09 13:30:00,57285,0.0,dedeira un,B&T+P&S,PetShop,Acessórios,2024,3,1.0,1.0,0.5,0.5
            2024-03-11 09:10:00,57297,0.0,dedeira un,B&T+P&S,PetShop,Acessórios,2024,3,1.0,1.0,0.5,0.5
            2024-07-09 16:03:00,59518,6438.0,dedeira un,B&T+P&S,PetShop,Acessórios,2024,7,1.0,1.0,1.0,1.0
            2024-08-08 15:13:00,60040,2354.0,peitoral para cachorros h nox lumen p,B&T+P&S,PetShop,Acessórios,2024,8,1.0,1.0,1.0,1.0
            2024-09-06 09:24:00,60526,6464.0,brinq cat guizo penas bulet bom amigo,B&T+P&S,PetShop,Acessórios,2024,9,1.0,1.0,1.0,1.0
            2024-09-12 16:22:00,60623,6073.0,mord. pelucia pop bicho lobo,B&T+P&S,PetShop,Acessórios,2024,9,1.0,1.0,1.0,1.0
            2024-09-21 12:22:00,60771,3624.0,mord pelucia macaco gd marrom kelev,B&T+P&S,PetShop,Acessórios,2024,9,1.0,1.0,1.0,1.0
            2024-10-10 09:16:00,61090,0.0,allequa 10ml,B&T+P&S,PetShop,Farmácia,2024,10,1.0,1.0,1.0,1.0
            2024-12-13 15:31:00,62069,4458.0,mord. pelucia pop bicho lobo,B&T+P&S,PetShop,Acessórios,2024,12,1.0,1.0,1.0,1.0
            2025-02-01 10:52:00,62808,2152.0,mord. pelucia pop bicho lobo,B&T+P&S,PetShop,Acessórios,2025,2,1.0,1.0,1.0,1.0
            2025-04-09 14:46:00,63779,5491.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais),Clí+,Internação,Diária,2025,4,1.0,1.0,1.0,1.0
            2023-02-24 15:49:00,49520,5237.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,2,1.0,1.0,1.0,1.0
            2023-05-19 14:51:00,51203,5745.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,5,1.0,1.0,1.0,1.0
            2023-05-26 12:25:00,51333,2152.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,5,1.0,1.0,1.0,1.0
            2023-06-19 14:14:00,51820,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,6,1.0,1.0,0.5,0.5
            2023-07-07 17:20:00,52221,5382.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,7,1.0,1.0,1.0,1.0
            2023-07-19 11:13:00,52431,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,7,0.5,0.5,0.3333333333333333,0.3333333333333333
            2023-07-19 11:13:00,52431,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,7,0.5,0.5,0.3333333333333333,0.3333333333333333
            2023-06-30 18:10:00,52084,2239.0,traumeel por ml,Clí+,Clínica,Procedimentos Clínico,2023,6,1.0,1.0,1.0,1.0
            2023-11-09 14:50:00,54817,2429.0,teste accuvet erliquiose,B&T+P&S,PetShop,Farmácia,2023,11,1.0,1.0,1.0,1.0
            2024-05-08 12:17:00,58329,5648.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais),Clí+,Internação,Diária,2024,5,1.0,1.0,0.3333333333333333,0.3333333333333333
            2024-05-09 12:02:00,58410,5648.0,vitalpet diária internação 11 a 15kg cod 424(sem medicamentos e materiais),Clí+,Internação,Diária,2024,5,1.0,1.0,0.3333333333333333,0.3333333333333333
            2024-09-12 14:38:00,60616,6155.0,internamento dia até 10kg,Clí+,Internação,Diária,2024,9,1.0,1.0,1.0,1.0
            2024-11-08 13:10:00,61579,3458.0,internamento dia até 10kg,Clí+,Internação,Diária,2024,11,1.0,1.0,1.0,1.0
            2023-01-18 18:18:00,48750,5842.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,1,1.0,1.0,1.0,1.0
            2023-01-21 11:32:00,48843,2152.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,1,1.0,1.0,1.0,1.0
            2023-06-02 08:31:00,51477,5858.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,6,1.0,1.0,1.0,1.0
            2023-01-11 13:57:00,48566,5825.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,1,1.0,1.0,1.0,1.0
            2023-01-13 17:45:00,48630,5827.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,1,1.0,1.0,1.0,1.0
            2023-01-21 12:17:00,48845,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,1,1.0,1.0,1.0,1.0
            2023-02-03 12:23:00,49125,3241.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,2,1.0,1.0,1.0,1.0
            2023-03-15 09:18:00,49916,2152.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,3,1.0,1.0,0.3333333333333333,0.3333333333333333
            2023-03-18 13:54:00,50015,5903.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,3,1.0,1.0,1.0,1.0
            2023-03-23 08:50:00,50075,2152.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,3,0.5,0.5,0.3333333333333333,0.3333333333333333
            2023-03-23 08:50:00,50075,2152.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,3,0.5,0.5,0.3333333333333333,0.3333333333333333
            2023-04-15 10:07:00,50541,5465.0,allequa 10ml,B&T+P&S,PetShop,Farmácia,2023,4,1.0,1.0,1.0,1.0
            2023-05-04 10:57:00,50882,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,5,1.0,1.0,1.0,1.0
            2023-06-06 09:53:00,51548,6023.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,6,1.0,1.0,1.0,1.0
            2023-06-30 12:36:00,52062,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,6,1.0,1.0,0.5,0.5
            2023-07-04 08:29:00,52121,5069.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,7,1.0,1.0,0.5,0.5
            2023-07-06 11:56:00,52180,2152.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,7,1.0,1.0,1.0,1.0
            2023-07-07 12:12:00,52206,0.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,7,1.0,1.0,0.5,0.5
            2023-07-11 10:39:00,52267,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,7,1.0,1.0,0.3333333333333333,0.3333333333333333
            2023-07-20 08:52:00,52442,5069.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,7,1.0,1.0,0.5,0.5
            2023-07-21 16:57:00,52506,0.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,7,1.0,1.0,0.5,0.5
            2023-07-29 08:59:00,52659,6081.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,7,1.0,1.0,1.0,1.0
            2023-09-14 13:37:00,53656,3049.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,9,1.0,1.0,1.0,1.0
            2023-12-06 09:09:00,55341,2152.0,retangular imperial gg estampado,B&T+P&S,PetShop,Farmácia,2023,12,0.5,0.5,0.5,0.5
            2023-12-06 09:09:00,55341,2152.0,retangular imperial gg estampado,B&T+P&S,PetShop,Farmácia,2023,12,0.5,0.5,0.5,0.5
            2024-01-22 17:48:00,56368,0.0,"feline satiety 1,5 kg - un",B&T+P&S,PetShop,Alimentos,2024,1,1.0,1.0,1.0,1.0
            2024-02-19 09:26:00,56853,5186.0,fitoclean 250ml,B&T+P&S,PetShop,Farmácia,2024,2,1.0,1.0,1.0,1.0
            2024-03-06 07:59:00,57186,6327.0,bolsa tricoline passeio (fofuchos pet),B&T+P&S,PetShop,Acessórios,2024,3,1.0,1.0,1.0,1.0
            2024-11-08 08:40:00,61563,2896.0,brinq cat guizo penas bulet bom amigo,B&T+P&S,PetShop,Acessórios,2024,11,1.0,1.0,1.0,1.0
            2023-01-20 16:21:00,48814,5853.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,1,1.0,1.0,1.0,1.0
            2023-05-06 16:26:00,50950,0.0,dedeira un,B&T+P&S,PetShop,Acessórios,2023,5,1.0,1.0,1.0,1.0
            2023-10-11 12:14:00,54227,0.0,bezzie risoto frango e vegetais 250gr,B&T+P&S,PetShop,Alimentos,2023,10,1.0,1.0,1.0,1.0
            2023-12-06 15:59:00,55359,4267.0,hydra groomers colonia forever gold 12,B&T+P&S,PetShop,Acessórios,2023,12,1.0,1.0,1.0,1.0
        """
        df_sales_enrich_expected = pd.read_csv(StringIO(sales_enrich_expected_csv))
        df_sales_enrich_expected["Data e hora"] = pd.to_datetime(df_sales_enrich_expected["Data e hora"])
        df_sales_enrich_expected = df_sales_enrich_expected
        df_sales_enrich_expected = df_sales_enrich_expected.astype({"__ano": "int32", "__mes": "int32"})

        pd.testing.assert_frame_equal(df_sales_enrich, df_sales_enrich_expected)

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

    def test_agg_pilar(self):
        sales_mock_csv = """Data e hora,Bruto,Quantidade,__categoria,__pilar,__ticket_por_pilar,__clientes_ativo_por_pilar
            2023-01-26 12:26:00,55.0,1.0,B&T+P&S,Banho e Tosa,0.5,0.25
            2023-01-11 13:57:00,5.0,1.0,B&T+P&S,PetShop,0.5,0.5
            2023-01-27 14:03:00,400.0,1.0,Clí+,Cirurgia,0.5,0.5
            2023-01-16 16:19:00,70.01,1.0,Clí+,Clínica,1.0,1.0
            2023-01-04 15:55:00,100.0,1.0,Clí+,Exames,0.5,0.5
            2023-01-03 11:37:00,200.0,1.0,Clí+,Internação,1.0,1.0
            2023-01-27 10:23:00,305.0,1.0,NULL,NULL,1.0,1.0
            2023-02-09 08:18:00,78.0,1.0,B&T+P&S,Banho e Tosa,0.5,0.5
            2023-02-14 12:24:00,33.9,1.0,B&T+P&S,PetShop,1.0,0.5
            2023-02-14 19:48:00,300.0,1.0,Clí+,Cirurgia,0.5,0.5
            2023-02-15 10:12:00,110.0,1.0,Clí+,Clínica,0.5,0.5
            2023-02-27 15:38:00,28.0,1.0,Clí+,Exames,0.125,0.125
            2023-02-03 11:30:00,80.0,1.0,Clí+,Internação,1.0,1.0
            2023-02-01 10:53:00,305.0,1.0,NULL,NULL,1.0,1.0
            2023-03-16 09:31:00,55.0,1.0,B&T+P&S,Banho e Tosa,0.5,0.06666666666666667
            2023-03-30 08:11:00,149.9,1.0,B&T+P&S,PetShop,1.0,1.0
            2023-03-04 09:17:00,380.0,1.0,Clí+,Cirurgia,0.5,0.5
            2023-03-08 09:08:00,130.0,1.0,Clí+,Clínica,1.0,1.0
            2023-03-04 08:26:00,140.0,1.0,Clí+,Exames,0.16666666666666666,0.09090909090909091
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])
        sales_mock_groupedby_pilar = df_sales_mock.groupby([pd.Grouper(key = 'Data e hora', freq = '1ME'), '__categoria' ,'__pilar'], dropna = False)

        agg_mock_pilar = get_agg_pilar_df(sales_mock_groupedby_pilar)
        agg_mock_pilar_flattened = agg_mock_pilar.unstack(level = -1).reset_index()

        agg_pilar_flattened_expected_csv = """level_0,__categoria,__pilar,Data e hora,0
            Faturamento Bruto,B&T+P&S,Banho e Tosa,2023-01-31,55.0
            Faturamento Bruto,B&T+P&S,Banho e Tosa,2023-02-28,78.0
            Faturamento Bruto,B&T+P&S,Banho e Tosa,2023-03-31,55.0
            Faturamento Bruto,B&T+P&S,PetShop,2023-01-31,5.0
            Faturamento Bruto,B&T+P&S,PetShop,2023-02-28,33.9
            Faturamento Bruto,B&T+P&S,PetShop,2023-03-31,149.9
            Faturamento Bruto,Clí+,Cirurgia,2023-01-31,400.0
            Faturamento Bruto,Clí+,Cirurgia,2023-02-28,300.0
            Faturamento Bruto,Clí+,Cirurgia,2023-03-31,380.0
            Faturamento Bruto,Clí+,Clínica,2023-01-31,70.01
            Faturamento Bruto,Clí+,Clínica,2023-02-28,110.0
            Faturamento Bruto,Clí+,Clínica,2023-03-31,130.0
            Faturamento Bruto,Clí+,Exames,2023-01-31,100.0
            Faturamento Bruto,Clí+,Exames,2023-02-28,28.0
            Faturamento Bruto,Clí+,Exames,2023-03-31,140.0
            Faturamento Bruto,Clí+,Internação,2023-01-31,200.0
            Faturamento Bruto,Clí+,Internação,2023-02-28,80.0
            Faturamento Bruto,Clí+,Internação,2023-03-31,0.0
            Faturamento Bruto,,,2023-01-31,305.0
            Faturamento Bruto,,,2023-02-28,305.0
            Faturamento Bruto,,,2023-03-31,0.0
            Quantidade Totalizada,B&T+P&S,Banho e Tosa,2023-01-31,1.0
            Quantidade Totalizada,B&T+P&S,Banho e Tosa,2023-02-28,1.0
            Quantidade Totalizada,B&T+P&S,Banho e Tosa,2023-03-31,1.0
            Quantidade Totalizada,B&T+P&S,PetShop,2023-01-31,1.0
            Quantidade Totalizada,B&T+P&S,PetShop,2023-02-28,1.0
            Quantidade Totalizada,B&T+P&S,PetShop,2023-03-31,1.0
            Quantidade Totalizada,Clí+,Cirurgia,2023-01-31,1.0
            Quantidade Totalizada,Clí+,Cirurgia,2023-02-28,1.0
            Quantidade Totalizada,Clí+,Cirurgia,2023-03-31,1.0
            Quantidade Totalizada,Clí+,Clínica,2023-01-31,1.0
            Quantidade Totalizada,Clí+,Clínica,2023-02-28,1.0
            Quantidade Totalizada,Clí+,Clínica,2023-03-31,1.0
            Quantidade Totalizada,Clí+,Exames,2023-01-31,1.0
            Quantidade Totalizada,Clí+,Exames,2023-02-28,1.0
            Quantidade Totalizada,Clí+,Exames,2023-03-31,1.0
            Quantidade Totalizada,Clí+,Internação,2023-01-31,1.0
            Quantidade Totalizada,Clí+,Internação,2023-02-28,1.0
            Quantidade Totalizada,Clí+,Internação,2023-03-31,0.0
            Quantidade Totalizada,,,2023-01-31,1.0
            Quantidade Totalizada,,,2023-02-28,1.0
            Quantidade Totalizada,,,2023-03-31,0.0
            Preço Médio,B&T+P&S,Banho e Tosa,2023-01-31,55.0
            Preço Médio,B&T+P&S,Banho e Tosa,2023-02-28,78.0
            Preço Médio,B&T+P&S,Banho e Tosa,2023-03-31,55.0
            Preço Médio,B&T+P&S,PetShop,2023-01-31,5.0
            Preço Médio,B&T+P&S,PetShop,2023-02-28,33.9
            Preço Médio,B&T+P&S,PetShop,2023-03-31,149.9
            Preço Médio,Clí+,Cirurgia,2023-01-31,400.0
            Preço Médio,Clí+,Cirurgia,2023-02-28,300.0
            Preço Médio,Clí+,Cirurgia,2023-03-31,380.0
            Preço Médio,Clí+,Clínica,2023-01-31,70.01
            Preço Médio,Clí+,Clínica,2023-02-28,110.0
            Preço Médio,Clí+,Clínica,2023-03-31,130.0
            Preço Médio,Clí+,Exames,2023-01-31,100.0
            Preço Médio,Clí+,Exames,2023-02-28,28.0
            Preço Médio,Clí+,Exames,2023-03-31,140.0
            Preço Médio,Clí+,Internação,2023-01-31,200.0
            Preço Médio,Clí+,Internação,2023-02-28,80.0
            Preço Médio,Clí+,Internação,2023-03-31,0.0
            Preço Médio,,,2023-01-31,305.0
            Preço Médio,,,2023-02-28,305.0
            Preço Médio,,,2023-03-31,0.0
            Tickets Médio,B&T+P&S,Banho e Tosa,2023-01-31,110.0
            Tickets Médio,B&T+P&S,Banho e Tosa,2023-02-28,156.0
            Tickets Médio,B&T+P&S,Banho e Tosa,2023-03-31,110.0
            Tickets Médio,B&T+P&S,PetShop,2023-01-31,10.0
            Tickets Médio,B&T+P&S,PetShop,2023-02-28,33.9
            Tickets Médio,B&T+P&S,PetShop,2023-03-31,149.9
            Tickets Médio,Clí+,Cirurgia,2023-01-31,800.0
            Tickets Médio,Clí+,Cirurgia,2023-02-28,600.0
            Tickets Médio,Clí+,Cirurgia,2023-03-31,760.0
            Tickets Médio,Clí+,Clínica,2023-01-31,70.01
            Tickets Médio,Clí+,Clínica,2023-02-28,220.0
            Tickets Médio,Clí+,Clínica,2023-03-31,130.0
            Tickets Médio,Clí+,Exames,2023-01-31,200.0
            Tickets Médio,Clí+,Exames,2023-02-28,224.0
            Tickets Médio,Clí+,Exames,2023-03-31,840.0000000000003
            Tickets Médio,Clí+,Internação,2023-01-31,200.0
            Tickets Médio,Clí+,Internação,2023-02-28,80.0
            Tickets Médio,Clí+,Internação,2023-03-31,0.0
            Tickets Médio,,,2023-01-31,305.0
            Tickets Médio,,,2023-02-28,305.0
            Tickets Médio,,,2023-03-31,0.0
            Faturamento Médio por Clientes,B&T+P&S,Banho e Tosa,2023-01-31,220.0
            Faturamento Médio por Clientes,B&T+P&S,Banho e Tosa,2023-02-28,156.0
            Faturamento Médio por Clientes,B&T+P&S,Banho e Tosa,2023-03-31,825.0000000000009
            Faturamento Médio por Clientes,B&T+P&S,PetShop,2023-01-31,10.0
            Faturamento Médio por Clientes,B&T+P&S,PetShop,2023-02-28,67.8
            Faturamento Médio por Clientes,B&T+P&S,PetShop,2023-03-31,149.9
            Faturamento Médio por Clientes,Clí+,Cirurgia,2023-01-31,800.0
            Faturamento Médio por Clientes,Clí+,Cirurgia,2023-02-28,600.0
            Faturamento Médio por Clientes,Clí+,Cirurgia,2023-03-31,760.0
            Faturamento Médio por Clientes,Clí+,Clínica,2023-01-31,70.01
            Faturamento Médio por Clientes,Clí+,Clínica,2023-02-28,220.0
            Faturamento Médio por Clientes,Clí+,Clínica,2023-03-31,130.0
            Faturamento Médio por Clientes,Clí+,Exames,2023-01-31,200.0
            Faturamento Médio por Clientes,Clí+,Exames,2023-02-28,224.0
            Faturamento Médio por Clientes,Clí+,Exames,2023-03-31,1540.0000000000002
            Faturamento Médio por Clientes,Clí+,Internação,2023-01-31,200.0
            Faturamento Médio por Clientes,Clí+,Internação,2023-02-28,80.0
            Faturamento Médio por Clientes,Clí+,Internação,2023-03-31,0.0
            Faturamento Médio por Clientes,,,2023-01-31,305.0
            Faturamento Médio por Clientes,,,2023-02-28,305.0
            Faturamento Médio por Clientes,,,2023-03-31,0.0
        """
        agg_pilar_flattened_expected = pd.read_csv(StringIO(agg_pilar_flattened_expected_csv))
        agg_pilar_flattened_expected = agg_pilar_flattened_expected.rename(columns={'0': 0})
        agg_pilar_flattened_expected["level_0"] = agg_pilar_flattened_expected["level_0"].str.strip()
        agg_pilar_flattened_expected["Data e hora"] = pd.to_datetime(agg_pilar_flattened_expected["Data e hora"])

        pd.testing.assert_frame_equal(agg_mock_pilar_flattened ,agg_pilar_flattened_expected)

    def test_agg_categoria(self):
        sales_mock_csv = """Data e hora,Bruto,Quantidade,__categoria
            2023-01-18 08:16:00,68.9,1.0,B&T+P&S
            2023-01-27 10:23:00,130.0,1.0,Clí+
            2023-01-04 18:21:00,276.0,1.0,NULL
            2023-02-10 11:00:00,15.0,1.0,B&T+P&S
            2023-02-15 13:15:00,320.0,1.0,Clí+
            2023-02-01 10:53:00,305.0,1.0,NULL
            2023-03-18 10:59:00,55.0,1.0,B&T+P&S
            2023-03-10 10:04:00,200.0,1.0,Clí+
            2023-03-04 09:09:00,30.0,1.0,NULL
            2023-04-04 11:40:00,57.9,1.0,B&T+P&S
            2023-04-11 16:00:00,60.0,1.0,Clí+
            2023-04-11 13:32:00,305.0,1.0,NULL
            2023-05-31 10:12:00,10.0,1.0,B&T+P&S
            2023-05-16 14:25:00,80.0,1.0,Clí+
            2023-05-23 13:53:00,260.0,1.0,NULL
            2023-06-12 13:54:00,8.0,1.0,B&T+P&S
            2023-06-01 08:49:00,38.0,1.0,Clí+
            2023-06-02 18:02:00,350.0,7.0,NULL
            2023-07-01 14:39:00,25.0,1.0,B&T+P&S
            2023-07-17 18:09:00,70.01,1.0,Clí+
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])
        sales_mock_groupedby_categoria = df_sales_mock.groupby([pd.Grouper(key = 'Data e hora', freq = '1ME'), '__categoria'], dropna = False)
        agg_categoria_mock = get_agg_categoria_df(sales_mock_groupedby_categoria)
        agg_categoria_mock_flattened = agg_categoria_mock.unstack(level = -1).reset_index()

        agg_categoria_mock_flattened_expected_csv = """level_0,__categoria,Data e hora,0
            Faturamento Bruto,B&T+P&S,2023-01-31,68.9
            Faturamento Bruto,B&T+P&S,2023-02-28,15.0
            Faturamento Bruto,B&T+P&S,2023-03-31,55.0
            Faturamento Bruto,B&T+P&S,2023-04-30,57.9
            Faturamento Bruto,B&T+P&S,2023-05-31,10.0
            Faturamento Bruto,B&T+P&S,2023-06-30,8.0
            Faturamento Bruto,B&T+P&S,2023-07-31,25.0
            Faturamento Bruto,Clí+,2023-01-31,130.0
            Faturamento Bruto,Clí+,2023-02-28,320.0
            Faturamento Bruto,Clí+,2023-03-31,200.0
            Faturamento Bruto,Clí+,2023-04-30,60.0
            Faturamento Bruto,Clí+,2023-05-31,80.0
            Faturamento Bruto,Clí+,2023-06-30,38.0
            Faturamento Bruto,Clí+,2023-07-31,70.01
            Faturamento Bruto,,2023-01-31,276.0
            Faturamento Bruto,,2023-02-28,305.0
            Faturamento Bruto,,2023-03-31,30.0
            Faturamento Bruto,,2023-04-30,305.0
            Faturamento Bruto,,2023-05-31,260.0
            Faturamento Bruto,,2023-06-30,350.0
            Faturamento Bruto,,2023-07-31,0.0
            Quantidade Totalizada,B&T+P&S,2023-01-31,1.0
            Quantidade Totalizada,B&T+P&S,2023-02-28,1.0
            Quantidade Totalizada,B&T+P&S,2023-03-31,1.0
            Quantidade Totalizada,B&T+P&S,2023-04-30,1.0
            Quantidade Totalizada,B&T+P&S,2023-05-31,1.0
            Quantidade Totalizada,B&T+P&S,2023-06-30,1.0
            Quantidade Totalizada,B&T+P&S,2023-07-31,1.0
            Quantidade Totalizada,Clí+,2023-01-31,1.0
            Quantidade Totalizada,Clí+,2023-02-28,1.0
            Quantidade Totalizada,Clí+,2023-03-31,1.0
            Quantidade Totalizada,Clí+,2023-04-30,1.0
            Quantidade Totalizada,Clí+,2023-05-31,1.0
            Quantidade Totalizada,Clí+,2023-06-30,1.0
            Quantidade Totalizada,Clí+,2023-07-31,1.0
            Quantidade Totalizada,,2023-01-31,1.0
            Quantidade Totalizada,,2023-02-28,1.0
            Quantidade Totalizada,,2023-03-31,1.0
            Quantidade Totalizada,,2023-04-30,1.0
            Quantidade Totalizada,,2023-05-31,1.0
            Quantidade Totalizada,,2023-06-30,7.0
            Quantidade Totalizada,,2023-07-31,0.0
            Preço Médio,B&T+P&S,2023-01-31,68.9
            Preço Médio,B&T+P&S,2023-02-28,15.0
            Preço Médio,B&T+P&S,2023-03-31,55.0
            Preço Médio,B&T+P&S,2023-04-30,57.9
            Preço Médio,B&T+P&S,2023-05-31,10.0
            Preço Médio,B&T+P&S,2023-06-30,8.0
            Preço Médio,B&T+P&S,2023-07-31,25.0
            Preço Médio,Clí+,2023-01-31,130.0
            Preço Médio,Clí+,2023-02-28,320.0
            Preço Médio,Clí+,2023-03-31,200.0
            Preço Médio,Clí+,2023-04-30,60.0
            Preço Médio,Clí+,2023-05-31,80.0
            Preço Médio,Clí+,2023-06-30,38.0
            Preço Médio,Clí+,2023-07-31,70.01
            Preço Médio,,2023-01-31,276.0
            Preço Médio,,2023-02-28,305.0
            Preço Médio,,2023-03-31,30.0
            Preço Médio,,2023-04-30,305.0
            Preço Médio,,2023-05-31,260.0
            Preço Médio,,2023-06-30,50.0
            Preço Médio,,2023-07-31,0.0
        """
        agg_categoria_mock_flattened_expected = pd.read_csv(StringIO(agg_categoria_mock_flattened_expected_csv))
        agg_categoria_mock_flattened_expected = agg_categoria_mock_flattened_expected.rename(columns={"0": 0})
        agg_categoria_mock_flattened_expected["level_0"] = agg_categoria_mock_flattened_expected["level_0"].str.strip()
        agg_categoria_mock_flattened_expected["Data e hora"] = pd.to_datetime(agg_categoria_mock_flattened_expected["Data e hora"])

        pd.testing.assert_frame_equal(agg_categoria_mock_flattened, agg_categoria_mock_flattened_expected)

    def test_agg_tempo(self):
        sales_mock_csv = """Data e hora,Bruto,Quantidade,__categoria,__pilar,__ticket,__clientes_ativos
            2023-01-11 11:43:00,5.9,1.0,B&T+P&S,PetShop,0.16666666666666666,0.1
            2023-01-18 18:18:00,14.9,1.0,B&T+P&S,PetShop,1.0,0.5
            2023-01-04 09:47:00,55.0,1.0,B&T+P&S,Banho e Tosa,0.25,0.14285714285714285
            2023-01-30 17:10:00,900.0,1.0,Clí+,Cirurgia,0.2,0.06666666666666667
            2023-01-07 10:31:00,110.0,1.0,B&T+P&S,Banho e Tosa,1.0,1.0
            2023-01-23 14:04:00,35.0,1.0,B&T+P&S,Banho e Tosa,0.5,0.5
            2023-01-26 17:22:00,55.0,1.0,B&T+P&S,Banho e Tosa,0.3333333333333333,0.2
            2023-01-25 14:11:00,70.01,1.0,Clí+,Clínica,0.3333333333333333,0.125
            2023-01-25 15:58:00,4.9,1.0,B&T+P&S,PetShop,0.3333333333333333,0.3333333333333333
            2023-01-05 09:07:00,35.0,1.0,B&T+P&S,Banho e Tosa,0.3333333333333333,0.041666666666666664
            2023-02-01 11:37:00,39.0,1.0,B&T+P&S,Banho e Tosa,0.3333333333333333,0.3333333333333333
            2023-02-13 11:46:00,10.0,1.0,B&T+P&S,PetShop,0.5,0.5
            2023-02-14 10:10:00,68.0,1.0,B&T+P&S,Banho e Tosa,1.0,1.0
            2023-02-14 17:04:00,59.9,1.0,B&T+P&S,PetShop,0.5,0.018518518518518517
            2023-02-11 09:03:00,55.0,1.0,B&T+P&S,Banho e Tosa,1.0,0.25
            2023-02-23 15:15:00,55.0,1.0,B&T+P&S,Banho e Tosa,1.0,0.3333333333333333
            2023-02-13 15:27:00,15.9,1.0,B&T+P&S,PetShop,0.3333333333333333,0.018518518518518517
            2023-02-10 10:11:00,36.9,1.0,B&T+P&S,PetShop,0.14285714285714285,0.14285714285714285
            2023-02-03 08:15:00,61.9,1.0,B&T+P&S,PetShop,0.3333333333333333,0.018518518518518517
            2023-02-15 09:40:00,120.0,1.0,Clí+,Clínica,1.0,1.0
            2023-03-22 08:28:00,250.0,1.0,Clí+,Clínica,0.2,0.2
            2023-03-14 16:02:00,100.0,1.0,B&T+P&S,Banho e Tosa,0.2,0.09090909090909091
            2023-03-11 11:16:00,55.0,1.0,B&T+P&S,Banho e Tosa,0.5,0.2
            2023-03-08 08:50:00,100.0,1.0,B&T+P&S,Banho e Tosa,1.0,0.09090909090909091
            2023-03-22 16:07:00,55.0,1.0,B&T+P&S,Banho e Tosa,0.5,0.125
            2023-03-16 11:28:00,10.0,2.0,B&T+P&S,Banho e Tosa,0.25,0.25
            2023-03-04 09:17:00,70.0,1.0,Clí+,Clínica,0.1111111111111111,0.1
            2023-03-04 08:52:00,56.3,1.0,B&T+P&S,PetShop,1.0,1.0
            2023-03-03 08:57:00,120.0,1.0,Clí+,Clínica,0.5,0.2
            2023-03-04 09:17:00,25.0,1.0,Clí+,Clínica,0.1111111111111111,0.1
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])

        sales_mock_groupedby_tempo = df_sales_mock \
                               .dropna(subset = ['__categoria', '__pilar'], how = "all") \
                               .groupby([pd.Grouper(key = 'Data e hora', freq = '1ME')])

        agg_tempo_mock = get_agg_tempo_df(sales_mock_groupedby_tempo)
        agg_tempo_mock_expected_csv = """Data e hora,Faturamento Bruto,Quantidade Totalizada,Preço Médio,Tickets Médio,Faturamento Médio por Clientes
            2023-01-31,1285.71,10.0,128.571,288.923595505618,427.21376582278486
            2023-02-28,521.6,10.0,52.160000000000004,84.91162790697676,144.28452250274427
            2023-03-31,841.3,11.0,76.48181818181818,192.41931385006353,356.9643201542912
        """

        agg_tempo_mock_expected = pd.read_csv(StringIO(agg_tempo_mock_expected_csv))
        agg_tempo_mock_expected["Data e hora"] = pd.to_datetime(agg_tempo_mock_expected["Data e hora"])
        agg_tempo_mock_expected = agg_tempo_mock_expected.set_index("Data e hora")
        agg_tempo_mock_expected.index = agg_tempo_mock_expected.index.to_period("M").to_timestamp("M")

        pd.testing.assert_frame_equal(agg_tempo_mock, agg_tempo_mock_expected)

    def test_exception(self):
        sales_mock_csv = """Data e hora,Quantidade,Bruto,__pilar,__grupo
            2023-01-21 10:16:00,1.0,55.0,Banho e Tosa,Banho
            2023-01-16 11:32:00,1.0,8.0,Banho e Tosa,Outros BT
            2023-01-17 17:07:00,1.0,35.0,Banho e Tosa,Tosa
            2023-01-31 16:02:00,2.0,30.0,Banho e Tosa,Transporte
            2023-01-27 13:55:00,1.0,500.0,Cirurgia,Cirurgia
            2023-01-04 18:21:00,1.0,100.0,Cirurgia,Procedimentos Cirurgico
            2023-01-17 11:49:00,1.0,120.0,Clínica,Consulta
            2023-01-23 11:58:00,1.0,25.0,Clínica,Procedimentos Clínico
            2023-01-16 16:08:00,1.0,80.0,Clínica,Vacina
            2023-01-20 14:23:00,1.0,320.0,Exames,Imagem
            2023-01-18 15:27:00,1.0,28.0,Exames,Laboratório
            2023-01-27 14:07:00,1.0,80.0,Internação,Diária
            2023-01-03 11:37:00,1.0,200.0,Internação,Procedimentos Internação
            2023-01-13 18:21:00,1.0,71.9,NULL,NULL
            2023-01-13 18:27:00,1.0,61.9,PetShop,Acessórios
            2023-01-14 09:03:00,1.0,13.9,PetShop,Alimentos
            2023-01-18 10:00:00,1.0,359.9,PetShop,Farmácia
            2023-02-15 09:26:00,1.0,55.0,Banho e Tosa,Banho
            2023-02-01 11:20:00,1.0,8.0,Banho e Tosa,Outros BT
            2023-02-15 16:03:00,1.0,15.0,Banho e Tosa,Tosa
            2023-02-27 11:49:00,1.0,10.0,Banho e Tosa,Transporte
            2023-02-27 16:49:00,1.0,727.8,Cirurgia,Cirurgia
            2023-02-14 19:48:00,1.0,380.0,Cirurgia,Procedimentos Cirurgico
            2023-02-27 11:49:00,1.0,120.0,Clínica,Consulta
            2023-02-03 11:25:00,1.0,65.0,Clínica,Procedimentos Clínico
            2023-02-09 09:06:00,1.0,80.0,Clínica,Vacina
            2023-02-15 13:15:00,1.0,320.0,Exames,Imagem
            2023-02-10 09:25:00,1.0,56.7,Exames,Laboratório
            2023-02-12 13:33:00,1.0,80.0,Internação,Diária
            2023-02-15 16:42:00,1.0,200.0,Internação,Procedimentos Internação
            2023-02-01 10:53:00,1.0,305.0,NULL,NULL
            2023-02-09 08:39:00,1.0,22.9,PetShop,Acessórios
            2023-02-28 17:00:00,2.0,15.8,PetShop,Alimentos
            2023-02-07 15:57:00,1.0,145.9,PetShop,Farmácia
            2023-03-07 10:12:00,1.0,55.0,Banho e Tosa,Banho
            2023-03-17 13:05:00,1.0,35.0,Banho e Tosa,Outros BT
            2023-03-30 08:15:00,1.0,30.0,Banho e Tosa,Tosa
            2023-03-30 13:01:00,1.0,5.0,Banho e Tosa,Transporte
            2023-03-08 12:03:00,1.0,300.0,Cirurgia,Cirurgia
            2023-03-17 15:07:00,1.0,600.0,Cirurgia,Procedimentos Cirurgico
            2023-03-24 15:52:00,1.0,100.0,Clínica,Consulta
            2023-03-01 17:56:00,2.0,20.0,Clínica,Procedimentos Clínico
            2023-03-15 14:41:00,1.0,290.0,Clínica,Vacina
            2023-03-07 17:44:00,1.0,180.0,Exames,Imagem
            2023-03-21 10:11:00,1.0,45.0,Exames,Laboratório
            2023-03-07 16:09:00,1.0,80.0,Internação,Diária
            2023-03-14 12:24:00,1.0,200.0,Internação,Procedimentos Internação
            2023-03-04 09:19:00,1.0,38.0,NULL,NULL
            2023-03-17 12:03:00,1.0,24.9,PetShop,Acessórios
            2023-03-24 10:58:00,4.0,23.6,PetShop,Alimentos
            2023-03-16 09:25:00,2.0,82.6,PetShop,Farmácia
        """
        df_sales_mock = pd.read_csv(StringIO(sales_mock_csv))
        df_sales_mock["Data e hora"] = pd.to_datetime(df_sales_mock["Data e hora"])
        grouped_sales_mock = df_sales_mock.groupby([pd.Grouper(key = "Data e hora", freq = '1ME'), "__pilar", "__grupo"])

        df_exception = get_exception_df(grouped_sales_mock)
        exception_expected_csv = """Data e hora,Consultas/Cirurgias,Consultas/Internação,Exames/Consultas
            2023-01-31,1.0,1.0,2.0
            2023-02-28,1.0,1.0,2.0
            2023-03-31,1.0,1.0,2.0
        """
        df_exception_expected = pd.read_csv(StringIO(exception_expected_csv))
        df_exception_expected = df_exception_expected.set_index("Data e hora")
        df_exception_expected.index = pd.to_datetime(df_exception_expected.index)
        df_exception_expected.index.freq = "ME"

        pd.testing.assert_frame_equal(df_exception, df_exception_expected)

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
            2024-02-29,
            2024-03-31,
            2024-04-30,
            2024-05-31,
            2024-06-30,
            2024-07-31,
            2024-08-31,
            2024-09-30,
            2024-10-31,
            2024-11-30,
            2024-12-31,
            2025-01-31,419.0
        """
        s_inadimpl_expected = pd.read_csv(StringIO(inadimpl_expected_csv))
        s_inadimpl_expected["Data e hora"] = pd.to_datetime(s_inadimpl_expected["Data e hora"])
        s_inadimpl_expected = s_inadimpl_expected.set_index("Data e hora")["Inadimplencia do Faturamento Bruto"]
        s_inadimpl_expected.index = s_inadimpl_expected.index.to_period("M").to_timestamp("M")

        pd.testing.assert_series_equal(s_inadimpl, s_inadimpl_expected)

