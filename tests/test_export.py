import unittest
from datetime import datetime
from io import StringIO
import ast
import pandas as pd

from etl_simplesvet.transformers.transform_pandas_export import med


class TestExport(unittest.TestCase):

    def test_med_ref(self):
        export_mock_csv = """CD_ID_ITEM,VL_MES,VL_ANO,VL_MED,TX_FX_VERD_INF_PREV,TX_FX_VERD_SUP,TX_FX_VERM_INF,TX_FX_VERM_SUP,TX_FX_CLNT_INF,TX_FX_CLNT_SUP,TX_ITEM,TX_IND,TX_USUA,TX_TIPO,TX_AUX,TX_TOTTX_IN_MED,TX_CLDR
            1646467,4,2025,,,,,,,,
            1646468,4,2025,,,,,,,,
            1646469,4,2025,,,,,,,,
            1646470,4,2025,,,,,,,,
            1646471,4,2025,,,,,,,,
            1646472,4,2025,,,,,,,,
            1646473,4,2025,,,,,,,,
            1646474,4,2025,,,,,,,,
            1646475,4,2025,,,,,,,,
            1646476,4,2025,,,,,,,,
            1646477,4,2025,,,,,,,,
            1646478,4,2025,,,,,,,,
            1646480,4,2025,,,,,,,,
            1646481,4,2025,,,,,,,,
            1646482,4,2025,,,,,,,,
            1646483,4,2025,,,,,,,,
            1646484,4,2025,,,,,,,,
            1646485,4,2025,,,,,,,,
            1646486,4,2025,,,,,,,,
            1646487,4,2025,,,,,,,,
            1646488,4,2025,,,,,,,,
            1646489,4,2025,,,,,,,,
            1646490,4,2025,,,,,,,,
            1646491,4,2025,,,,,,,,
            1646492,4,2025,,,,,,,,
            1646493,4,2025,,,,,,,,
            1646494,4,2025,,,,,,,,
            1646496,4,2025,,,,,,,,
            1646497,4,2025,,,,,,,,
            1646498,4,2025,,,,,,,,
            1646499,4,2025,,,,,,,,
            1646500,4,2025,,,,,,,,
            1646501,4,2025,,,,,,,,
            1646502,4,2025,,,,,,,,
            1646503,4,2025,,,,,,,,
            1646504,4,2025,,,,,,,,
            1646505,4,2025,,,,,,,,
            1646652,4,2025,,,,,,,,
            1646506,4,2025,,,,,,,,
            1646509,4,2025,,,,,,,,
            1646510,4,2025,,,,,,,,
            1646511,4,2025,,,,,,,,
            1646512,4,2025,,,,,,,,
            1646513,4,2025,,,,,,,,
            1646514,4,2025,,,,,,,,
            1646515,4,2025,,,,,,,,
            1646516,4,2025,,,,,,,,
            1646517,4,2025,,,,,,,,
            1646518,4,2025,,,,,,,,
            1646519,4,2025,,,,,,,,
            1646520,4,2025,,,,,,,,
            1646521,4,2025,,,,,,,,
            1646522,4,2025,,,,,,,,
            1646523,4,2025,,,,,,,,
            1646525,4,2025,,,,,,,,
            1646526,4,2025,,,,,,,,
            1646527,4,2025,,,,,,,,
            1646528,4,2025,,,,,,,,
            1646529,4,2025,,,,,,,,
            1646530,4,2025,,,,,,,,
            1646531,4,2025,,,,,,,,
            1646532,4,2025,,,,,,,,
            1646654,4,2025,,,,,,,,
            1646533,4,2025,,,,,,,,
            1646534,4,2025,,,,,,,,
            1646535,4,2025,,,,,,,,
            1646536,4,2025,,,,,,,,
            1646537,4,2025,,,,,,,,
            1646538,4,2025,,,,,,,,
            1646539,4,2025,,,,,,,,
            1646540,4,2025,,,,,,,,
            1646541,4,2025,,,,,,,,
            1646542,4,2025,,,,,,,,
            1646543,4,2025,,,,,,,,
            1646544,4,2025,,,,,,,,
            1646545,4,2025,,,,,,,,
            1646546,4,2025,,,,,,,,
            1646547,4,2025,,,,,,,,
            1646548,4,2025,,,,,,,,
            1646550,4,2025,,,,,,,,
            1646551,4,2025,,,,,,,,
            1646552,4,2025,,,,,,,,
            1646553,4,2025,,,,,,,,
            1646554,4,2025,,,,,,,,
            1646653,4,2025,,,,,,,,
            1646555,4,2025,,,,,,,,
            1646556,4,2025,,,,,,,,
            1646557,4,2025,,,,,,,,
            1646558,4,2025,,,,,,,,
            1646573,4,2025,,,,,,,,
            1646479,4,2025,,,,,,,,
            1646507,4,2025,,,,,,,,
            1646508,4,2025,,,,,,,,
            1646559,4,2025,,,,,,,,
            1646560,4,2025,,,,,,,,
            1646561,4,2025,,,,,,,,
            1646562,4,2025,,,,,,,,
            1646563,4,2025,,,,,,,,
            1646564,4,2025,,,,,,,,
            1646565,4,2025,,,,,,,,
            1646566,4,2025,,,,,,,,
            1646567,4,2025,,,,,,,,
            1646568,4,2025,,,,,,,,
            1646569,4,2025,,,,,,,,
            1646570,4,2025,,,,,,,,
            1646571,4,2025,,,,,,,,
            1646572,4,2025,,,,,,,,
        """

        mapping_item_mock_csv = """CD_ID_ITEM,TX_CAT,TX_PIL,TX_GRP,TX_OP,TX_OP_EXCE,VL_MLTP
            1646467,x,x,Campanha,Quantidade Totalizada Clientes,x,1
            1646468,x,x,Indicação Parceiros,Quantidade Totalizada Clientes,x,1
            1646469,x,x,Facebook/Instagram,Quantidade Totalizada Clientes,x,1
            1646470,x,x,Fachada,Quantidade Totalizada Clientes,x,1
            1646471,x,x,Google,Quantidade Totalizada Clientes,x,1
            1646472,x,x,Indicação Clientes,Quantidade Totalizada Clientes,x,1
            1646473,x,x,Indicação Funcionarios,Quantidade Totalizada Clientes,x,1
            1646474,x,x,Outros,Quantidade Totalizada Clientes,x,1
            1646475,x,x,x,Quantidade Totalizada Clientes Ativos,x,1
            1646476,x,x,x,Quantidade Totalizada Clientes,x,1
            1646477,x,x,x,x,Consultas/Cirurgias,1
            1646478,x,x,x,x,Consultas/Internação,1
            1646479,x,x,x,x,x,1
            1646480,x,x,x,x,Exames/Consultas,1
            1646481,x,PetShop,Acessórios,Faturamento Bruto,x,1
            1646482,x,PetShop,Alimentos,Faturamento Bruto,x,1
            1646483,B&T+P&S,Banho e Tosa,x,Faturamento Bruto,x,1
            1646484,B&T+P&S,x,x,Faturamento Bruto,x,1
            1646485,x,Banho e Tosa,Banho,Faturamento Bruto,x,1
            1646486,Clí+,Cirurgia,x,Faturamento Bruto,x,1
            1646487,x,Cirurgia,Cirurgia,Faturamento Bruto,x,1
            1646488,Clí+,x,x,Faturamento Bruto,x,1
            1646489,Clí+,Clínica,x,Faturamento Bruto,x,1
            1646490,x,Clínica,Consulta,Faturamento Bruto,x,1
            1646491,x,Internação,Diária,Faturamento Bruto,x,1
            1646492,Clí+,Exames,x,Faturamento Bruto,x,1
            1646493,x,Exames,Imagem,Faturamento Bruto,x,1
            1646494,x,Exames,Laboratório,Faturamento Bruto,x,1
            1646496,Clí+,Internação,x,Faturamento Bruto,x,1
            1646497,x,PetShop,Farmácia,Faturamento Bruto,x,1
            1646498,x,x,x,Faturamento Médio por Clientes,x,1
            1646499,x,Banho e Tosa,Outros BT,Faturamento Bruto,x,1
            1646500,B&T+P&S,PetShop,x,Faturamento Bruto,x,1
            1646501,x,Cirurgia,Procedimentos Cirurgico,Faturamento Bruto,x,1
            1646502,x,Clínica,Procedimentos Clínico,Faturamento Bruto,x,1
            1646503,x,Internação,Procedimentos Internação,Faturamento Bruto,x,1
            1646504,x,Banho e Tosa,Tosa,Faturamento Bruto,x,1
            1646505,x,x,x,Faturamento Bruto,x,1
            1646652,x,Banho e Tosa,Transporte,Faturamento Bruto,x,1
            1646506,x,Clínica,Vacina,Faturamento Bruto,x,1
            1646507,x,x,x,x,x,1
            1646508,x,x,x,x,Inadimplencia do Faturamento Bruto,1
            1646509,x,x,x,Preço Médio,x,1
            1646510,x,PetShop,Acessórios,Preço Médio,x,1
            1646511,x,PetShop,Alimentos,Preço Médio,x,1
            1646512,B&T+P&S,Banho e Tosa,x,Preço Médio,x,1
            1646513,B&T+P&S,x,x,Preço Médio,x,1
            1646514,x,Banho e Tosa,Banho,Preço Médio,x,1
            1646515,Clí+,Cirurgia,x,Preço Médio,x,1
            1646516,x,Cirurgia,Cirurgia,Preço Médio,x,1
            1646517,Clí+,x,x,Preço Médio,x,1
            1646518,Clí+,Clínica,x,Preço Médio,x,1
            1646519,x,Clínica,Consulta,Preço Médio,x,1
            1646520,x,Internação,Diária,Preço Médio,x,1
            1646521,Clí+,Exames,x,Preço Médio,x,1
            1646522,x,Exames,Imagem,Preço Médio,x,1
            1646523,x,Exames,Laboratório,Preço Médio,x,1
            1646525,Clí+,Internação,x,Preço Médio,x,1
            1646526,x,PetShop,Farmácia,Preço Médio,x,1
            1646527,x,Banho e Tosa,Outros BT,Preço Médio,x,1
            1646528,B&T+P&S,PetShop,x,Preço Médio,x,1
            1646529,x,Cirurgia,Procedimentos Cirurgico,Preço Médio,x,1
            1646530,x,Clínica,Procedimentos Clínico,Preço Médio,x,1
            1646531,x,Internação,Procedimentos Internação,Preço Médio,x,1
            1646532,x,Banho e Tosa,Tosa,Preço Médio,x,1
            1646654,x,Banho e Tosa,Transporte,Preço Médio,x,1
            1646533,x,Clínica,Vacina,Preço Médio,x,1
            1646534,x,Clínica,Consulta,Quantidade Totalizada,x,1
            1646535,x,Clínica,Vacina,Quantidade Totalizada,x,1
            1646536,B&T+P&S,Banho e Tosa,x,Quantidade Totalizada,x,1
            1646537,B&T+P&S,x,x,Quantidade Totalizada,x,1
            1646538,x,Banho e Tosa,Banho,Quantidade Totalizada,x,1
            1646539,Clí+,Cirurgia,x,Quantidade Totalizada,x,1
            1646540,x,Cirurgia,Cirurgia,Quantidade Totalizada,x,1
            1646541,x,Cirurgia,Procedimentos Cirurgico,Quantidade Totalizada,x,1
            1646542,Clí+,x,x,Quantidade Totalizada,x,1
            1646543,Clí+,Clínica,x,Quantidade Totalizada,x,1
            1646544,x,Clínica,Procedimentos Clínico,Quantidade Totalizada,x,1
            1646545,x,Internação,Diária,Quantidade Totalizada,x,1
            1646546,Clí+,Exames,x,Quantidade Totalizada,x,1
            1646547,x,Exames,Imagem,Quantidade Totalizada,x,1
            1646548,x,Exames,Laboratório,Quantidade Totalizada,x,1
            1646550,Clí+,Internação,x,Quantidade Totalizada,x,1
            1646551,x,Internação,Procedimentos Internação,Quantidade Totalizada,x,1
            1646552,x,Banho e Tosa,Outros BT,Quantidade Totalizada,x,1
            1646553,x,x,x,Quantidade Totalizada,x,1
            1646554,x,Banho e Tosa,Tosa,Quantidade Totalizada,x,1
            1646653,x,Banho e Tosa,Transporte,Quantidade Totalizada,x,1
            1646555,x,PetShop,Acessórios,Quantidade Totalizada,x,1
            1646556,x,PetShop,Alimentos,Quantidade Totalizada,x,1
            1646557,x,PetShop,Farmácia,Quantidade Totalizada,x,1
            1646558,B&T+P&S,PetShop,x,Quantidade Totalizada,x,1
            1646559,x,x,x,x,x,1
            1646560,x,x,x,x,x,1
            1646561,x,x,x,x,x,1
            1646562,x,x,x,x,x,1
            1646563,x,x,x,x,x,1
            1646564,x,x,x,x,x,1
            1646565,x,x,x,x,x,1
            1646566,x,x,x,x,x,1
            1646567,x,x,x,x,x,1
            1646568,x,x,x,x,x,1
            1646569,x,x,x,x,x,1
            1646570,x,x,x,x,x,1
            1646571,x,x,x,x,x,1
            1646572,x,x,x,x,x,1
            1646573,x,x,x,Tickets Médio,x,1
            0,B&T+P&S,x,x,x,x,1
            1,Clí+,x,x,x,x,1
            2,x,Cirurgia,x,x,x,1
            3,x,Exames,x,x,x,1
            4,x,Clínica,x,x,x,1
            5,x,Internação,x,x,x,1
            6,x,PetShop,x,x,x,1
            7,x,Banho e Tosa,x,x,x,1
            8,x,x,Cirurgia,x,x,1
            9,x,x,Outros BT,x,x,1
            10,x,x,Vacina,x,x,1
            11,x,x,Procedimentos Clínico,x,x,1
            12,x,x,Farmácia,x,x,1
            13,x,x,Procedimentos Internação,x,x,1
            14,x,x,Tosa,x,x,1
            15,x,x,Acessórios,x,x,1
            16,x,x,Laboratório,x,x,1
            17,x,x,Consulta,x,x,1
            18,x,x,Banho,x,x,1
            19,x,x,Transporte,x,x,1
            20,x,x,Imagem,x,x,1
            21,x,x,Alimentos,x,x,1
            22,x,x,Diária,x,x,1
            23,x,x,Procedimentos Cirurgico,x,x,1
            24,x,x,x,Faturamento Bruto,x,1
            25,x,x,x,Faturamento Médio por Clientes,x,1
            26,x,x,x,Preço Médio,x,1
            27,x,x,x,Quantidade Totalizada,x,1
            28,x,x,x,Tickets Médio,x,1
            29,x,x,x,x,Consultas/Cirurgias,1
            30,x,x,x,x,Consultas/Internação,1
            31,x,x,x,x,Exames/Consultas,1
        """

        meds_mock_csv = """TS_DT_HR_VND,VL_MED,TX_OPS
            2025-04-30,46.0,"('Consultas/Cirurgias',)"
            2025-03-31,158.0,"('Consultas/Cirurgias',)"
            2025-04-30,59.0,"('Consultas/Internação',)"
            2025-02-28,4.492753623188406,"('Exames/Consultas',)"
            2025-04-30,3.492753623188406,"('Exames/Consultas',)"
            2025-04-30,116504.62,"('Faturamento Bruto',)"
            2025-04-30,53614.53,"('Faturamento Bruto', 'B&T+P&S')"
            2025-04-30,23633.0,"('Faturamento Bruto', 'B&T+P&S', 'Banho e Tosa')"
            2025-04-30,31490.33,"('Faturamento Bruto', 'B&T+P&S', 'PetShop')"
            2025-04-30,18673.6,"('Faturamento Bruto', 'Banho e Tosa', 'Banho')"
            2025-04-30,1166.0,"('Faturamento Bruto', 'Banho e Tosa', 'Outros BT')"
            2025-04-30,4262.0,"('Faturamento Bruto', 'Banho e Tosa', 'Tosa')"
            2025-04-30,370.0,"('Faturamento Bruto', 'Banho e Tosa', 'Transporte')"
            2025-04-30,14810.53,"('Faturamento Bruto', 'Cirurgia', 'Cirurgia')"
            2025-04-30,8796.57,"('Faturamento Bruto', 'Cirurgia', 'Procedimentos Cirurgico')"
            2025-04-30,63422.98,"('Faturamento Bruto', 'Clí+')"
            2025-04-30,22590.53,"('Faturamento Bruto', 'Clí+', 'Cirurgia')"
            2025-04-30,25485.71,"('Faturamento Bruto', 'Clí+', 'Clínica')"
            2025-04-30,20483.8,"('Faturamento Bruto', 'Clí+', 'Exames')"
            2025-04-30,6957.0,"('Faturamento Bruto', 'Clí+', 'Internação')"
            2025-04-30,13840.11,"('Faturamento Bruto', 'Clínica', 'Consulta')"
            2025-04-30,5788.849999999999,"('Faturamento Bruto', 'Clínica', 'Procedimentos Clínico')"
            2025-04-30,6501.6,"('Faturamento Bruto', 'Clínica', 'Vacina')"
            2025-04-30,4440.0,"('Faturamento Bruto', 'Exames', 'Imagem')"
            2025-04-30,16043.8,"('Faturamento Bruto', 'Exames', 'Laboratório')"
            2025-04-30,6808.0,"('Faturamento Bruto', 'Internação', 'Diária')"
            2025-04-30,641.0,"('Faturamento Bruto', 'Internação', 'Procedimentos Internação')"
            2025-04-30,0.0,"('Faturamento Bruto', 'NULL')"
            2025-04-30,0.0,"('Faturamento Bruto', 'NULL', 'NULL')"
            2025-04-30,8032.0,"('Faturamento Bruto', 'PetShop', 'Acessórios')"
            2025-04-30,4122.23,"('Faturamento Bruto', 'PetShop', 'Alimentos')"
            2025-04-30,19336.1,"('Faturamento Bruto', 'PetShop', 'Farmácia')"
            2025-04-30,557.4383732057416,"('Faturamento Médio por Clientes',)"
            2025-04-30,156.909219858156,"('Faturamento Médio por Clientes', 'B&T+P&S', 'Banho e Tosa')"
            2025-04-30,328.0242708333333,"('Faturamento Médio por Clientes', 'B&T+P&S', 'PetShop')"
            2025-04-30,2823.81625,"('Faturamento Médio por Clientes', 'Clí+', 'Cirurgia')"
            2025-04-30,299.5008450704225,"('Faturamento Médio por Clientes', 'Clí+', 'Clínica')"
            2025-04-30,426.7458333333333,"('Faturamento Médio por Clientes', 'Clí+', 'Exames')"
            2025-04-30,773.0,"('Faturamento Médio por Clientes', 'Clí+', 'Internação')"
            2025-04-30,0.0,"('Faturamento Médio por Clientes', 'NULL', 'NULL')"
            2025-04-30,30461.92,"('Inadimplencia do Faturamento Bruto',)"
            2025-04-30,92.83236653386454,"('Preço Médio',)"
            2025-04-30,60.19588235294118,"('Preço Médio', 'B&T+P&S')"
            2025-04-30,52.16916099773243,"('Preço Médio', 'B&T+P&S', 'Banho e Tosa')"
            2025-04-30,69.65080487804879,"('Preço Médio', 'B&T+P&S', 'PetShop')"
            2025-04-30,68.36283185840708,"('Preço Médio', 'Banho e Tosa', 'Banho')"
            2025-04-30,26.4425,"('Preço Médio', 'Banho e Tosa', 'Outros BT')"
            2025-04-30,36.11864406779661,"('Preço Médio', 'Banho e Tosa', 'Tosa')"
            2025-04-30,11.58620689655172,"('Preço Médio', 'Banho e Tosa', 'Transporte')"
            2025-04-30,871.2076470588236,"('Preço Médio', 'Cirurgia', 'Cirurgia')"
            2025-04-30,517.445294117647,"('Preço Médio', 'Cirurgia', 'Procedimentos Cirurgico')"
            2025-04-30,185.5164896755162,"('Preço Médio', 'Clí+')"
            2025-04-30,645.4437142857142,"('Preço Médio', 'Clí+', 'Cirurgia')"
            2025-04-30,157.7945192307693,"('Preço Médio', 'Clí+', 'Clínica')"
            2025-04-30,132.9655172413793,"('Preço Médio', 'Clí+', 'Exames')"
            2025-04-30,366.1578947368421,"('Preço Médio', 'Clí+', 'Internação')"
            2025-04-30,192.3919565217391,"('Preço Médio', 'Clínica', 'Consulta')"
            2025-04-30,166.71,"('Preço Médio', 'Clínica', 'Procedimentos Clínico')"
            2025-04-30,111.4,"('Preço Médio', 'Clínica', 'Vacina')"
            2025-04-30,371.725,"('Preço Médio', 'Exames', 'Imagem')"
            2025-04-30,116.0961538461538,"('Preço Médio', 'Exames', 'Laboratório')"
            2025-04-30,490.0,"('Preço Médio', 'Internação', 'Diária')"
            2025-04-30,91.57142857142857,"('Preço Médio', 'Internação', 'Procedimentos Internação')"
            2025-04-30,0.0,"('Preço Médio', 'NULL')"
            2025-04-30,0.0,"('Preço Médio', 'NULL', 'NULL')"
            2025-04-30,67.49579831932773,"('Preço Médio', 'PetShop', 'Acessórios')"
            2025-04-30,52.37507462686568,"('Preço Médio', 'PetShop', 'Alimentos')"
            2025-04-30,84.33849765258215,"('Preço Médio', 'PetShop', 'Farmácia')"
            2025-04-30,1299.0,"('Quantidade Totalizada',)"
            2025-04-30,916.0,"('Quantidade Totalizada', 'B&T+P&S')"
            2025-04-30,458.0,"('Quantidade Totalizada', 'B&T+P&S', 'Banho e Tosa')"
            2025-04-30,458.0,"('Quantidade Totalizada', 'B&T+P&S', 'PetShop')"
            2025-04-30,275.0,"('Quantidade Totalizada', 'Banho e Tosa', 'Banho')"
            2025-04-30,74.0,"('Quantidade Totalizada', 'Banho e Tosa', 'Outros BT')"
            2025-04-30,118.0,"('Quantidade Totalizada', 'Banho e Tosa', 'Tosa')"
            2025-04-30,37.0,"('Quantidade Totalizada', 'Banho e Tosa', 'Transporte')"
            2025-04-30,17.0,"('Quantidade Totalizada', 'Cirurgia', 'Cirurgia')"
            2025-04-30,20.0,"('Quantidade Totalizada', 'Cirurgia', 'Procedimentos Cirurgico')"
            2025-04-30,445.0,"('Quantidade Totalizada', 'Clí+')"
            2025-04-30,35.0,"('Quantidade Totalizada', 'Clí+', 'Cirurgia')"
            2025-04-30,192.0,"('Quantidade Totalizada', 'Clí+', 'Clínica')"
            2025-04-30,241.0,"('Quantidade Totalizada', 'Clí+', 'Exames')"
            2025-04-30,19.0,"('Quantidade Totalizada', 'Clí+', 'Internação')"
            2025-04-30,85.0,"('Quantidade Totalizada', 'Clínica', 'Consulta')"
            2025-04-30,57.0,"('Quantidade Totalizada', 'Clínica', 'Procedimentos Clínico')"
            2025-04-30,60.0,"('Quantidade Totalizada', 'Clínica', 'Vacina')"
            2025-04-30,18.0,"('Quantidade Totalizada', 'Exames', 'Imagem')"
            2025-04-30,228.0,"('Quantidade Totalizada', 'Exames', 'Laboratório')"
            2025-04-30,16.0,"('Quantidade Totalizada', 'Internação', 'Diária')"
            2025-04-30,7.0,"('Quantidade Totalizada', 'Internação', 'Procedimentos Internação')"
            2025-04-30,0.0,"('Quantidade Totalizada', 'NULL')"
            2025-04-30,0.0,"('Quantidade Totalizada', 'NULL', 'NULL')"
            2025-04-30,130.0,"('Quantidade Totalizada', 'PetShop', 'Acessórios')"
            2025-04-30,94.0,"('Quantidade Totalizada', 'PetShop', 'Alimentos')"
            2025-04-30,255.0,"('Quantidade Totalizada', 'PetShop', 'Farmácia')"
            2025-04-30,30.0,"('Quantidade Totalizada Clientes',)"
            2025-04-30,0.0,"('Quantidade Totalizada Clientes', 'Campanha')"
            2025-04-30,2.0,"('Quantidade Totalizada Clientes', 'Facebook/Instagram')"
            2025-04-30,16.0,"('Quantidade Totalizada Clientes', 'Fachada')"
            2025-04-30,8.0,"('Quantidade Totalizada Clientes', 'Google')"
            2025-04-30,6.0,"('Quantidade Totalizada Clientes', 'Indicação Clientes')"
            2025-04-30,3.0,"('Quantidade Totalizada Clientes', 'Indicação Funcionarios')"
            2025-04-30,5.0,"('Quantidade Totalizada Clientes', 'Indicação Parceiros')"
            2025-04-30,4.0,"('Quantidade Totalizada Clientes', 'Outros')"
            2025-04-30,625.0,"('Quantidade Totalizada Clientes Ativos',)"
            2025-04-30,276.077298578199,"('Tickets Médio',)"
            2025-04-30,97.2551440329218,"('Tickets Médio', 'B&T+P&S', 'Banho e Tosa')"
            2025-04-30,184.1539766081872,"('Tickets Médio', 'B&T+P&S', 'PetShop')"
            2025-04-30,1737.733076923077,"('Tickets Médio', 'Clí+', 'Cirurgia')"
            2025-04-30,224.8031506849315,"('Tickets Médio', 'Clí+', 'Clínica')"
            2025-04-30,367.2380952380952,"('Tickets Médio', 'Clí+', 'Exames')"
            2025-04-30,632.4545454545455,"('Tickets Médio', 'Clí+', 'Internação')"
            2025-04-30,0.0,"('Tickets Médio', 'NULL', 'NULL')"
        """

        df_export_mock = pd.read_csv(StringIO(export_mock_csv)).set_index("CD_ID_ITEM").astype({"VL_MED": str})

        df_mapping_item_mock = pd.read_csv(StringIO(mapping_item_mock_csv)).set_index("CD_ID_ITEM")
        df_meds_mock = pd.read_csv(StringIO(meds_mock_csv), converters = {"TX_OPS": ast.literal_eval})
        df_meds_mock["TS_DT_HR_VND"] = pd.to_datetime(df_meds_mock["TS_DT_HR_VND"])
        end_date = datetime(2025, 5, 1, 0, 0)
        df_export_meds = med(df_export_mock, df_meds_mock, df_mapping_item_mock, end_date)

        export_expected_csv = """CD_ID_ITEM,VL_MES,VL_ANO,VL_MED,TX_FX_VERD_INF_PREV,TX_FX_VERD_SUP,TX_FX_VERM_INF,TX_FX_VERM_SUP,TX_FX_CLNT_INF,TX_FX_CLNT_SUP,TX_ITEM,TX_IND,TX_USUA,TX_TIPO,TX_AUX,TX_TOTTX_IN_MED,TX_CLDR
            1646467,4,2025,0.0,,,,,,
            1646468,4,2025,5.0,,,,,,
            1646469,4,2025,2.0,,,,,,
            1646470,4,2025,16.0,,,,,,
            1646471,4,2025,8.0,,,,,,
            1646472,4,2025,6.0,,,,,,
            1646473,4,2025,3.0,,,,,,
            1646474,4,2025,4.0,,,,,,
            1646475,4,2025,625.0,,,,,,
            1646476,4,2025,30.0,,,,,,
            1646477,4,2025,46.0,,,,,,
            1646478,4,2025,59.0,,,,,,
            1646480,4,2025,3.492753623188406,,,,,,
            1646481,4,2025,8032.0,,,,,,
            1646482,4,2025,4122.23,,,,,,
            1646483,4,2025,23633.0,,,,,,
            1646484,4,2025,53614.53,,,,,,
            1646485,4,2025,18673.6,,,,,,
            1646486,4,2025,22590.53,,,,,,
            1646487,4,2025,14810.53,,,,,,
            1646488,4,2025,63422.98,,,,,,
            1646489,4,2025,25485.71,,,,,,
            1646490,4,2025,13840.11,,,,,,
            1646491,4,2025,6808.0,,,,,,
            1646492,4,2025,20483.8,,,,,,
            1646493,4,2025,4440.0,,,,,,
            1646494,4,2025,16043.8,,,,,,
            1646496,4,2025,6957.0,,,,,,
            1646497,4,2025,19336.1,,,,,,
            1646498,4,2025,557.4383732057416,,,,,,
            1646499,4,2025,1166.0,,,,,,
            1646500,4,2025,31490.33,,,,,,
            1646501,4,2025,8796.57,,,,,,
            1646502,4,2025,5788.849999999999,,,,,,
            1646503,4,2025,641.0,,,,,,
            1646504,4,2025,4262.0,,,,,,
            1646505,4,2025,116504.62,,,,,,
            1646652,4,2025,370.0,,,,,,
            1646506,4,2025,6501.6,,,,,,
            1646509,4,2025,92.83236653386454,,,,,,
            1646510,4,2025,67.49579831932773,,,,,,
            1646511,4,2025,52.37507462686568,,,,,,
            1646512,4,2025,52.16916099773243,,,,,,
            1646513,4,2025,60.19588235294118,,,,,,
            1646514,4,2025,68.36283185840708,,,,,,
            1646515,4,2025,645.4437142857142,,,,,,
            1646516,4,2025,871.2076470588236,,,,,,
            1646517,4,2025,185.5164896755162,,,,,,
            1646518,4,2025,157.7945192307693,,,,,,
            1646519,4,2025,192.3919565217391,,,,,,
            1646520,4,2025,490.0,,,,,,
            1646521,4,2025,132.9655172413793,,,,,,
            1646522,4,2025,371.725,,,,,,
            1646523,4,2025,116.0961538461538,,,,,,
            1646525,4,2025,366.1578947368421,,,,,,
            1646526,4,2025,84.33849765258215,,,,,,
            1646527,4,2025,26.4425,,,,,,
            1646528,4,2025,69.65080487804879,,,,,,
            1646529,4,2025,517.445294117647,,,,,,
            1646530,4,2025,166.71,,,,,,
            1646531,4,2025,91.57142857142856,,,,,,
            1646532,4,2025,36.11864406779661,,,,,,
            1646654,4,2025,11.58620689655172,,,,,,
            1646533,4,2025,111.4,,,,,,
            1646534,4,2025,85.0,,,,,,
            1646535,4,2025,60.0,,,,,,
            1646536,4,2025,458.0,,,,,,
            1646537,4,2025,916.0,,,,,,
            1646538,4,2025,275.0,,,,,,
            1646539,4,2025,35.0,,,,,,
            1646540,4,2025,17.0,,,,,,
            1646541,4,2025,20.0,,,,,,
            1646542,4,2025,445.0,,,,,,
            1646543,4,2025,192.0,,,,,,
            1646544,4,2025,57.0,,,,,,
            1646545,4,2025,16.0,,,,,,
            1646546,4,2025,241.0,,,,,,
            1646547,4,2025,18.0,,,,,,
            1646548,4,2025,228.0,,,,,,
            1646550,4,2025,19.0,,,,,,
            1646551,4,2025,7.0,,,,,,
            1646552,4,2025,74.0,,,,,,
            1646553,4,2025,1299.0,,,,,,
            1646554,4,2025,118.0,,,,,,
            1646653,4,2025,37.0,,,,,,
            1646555,4,2025,130.0,,,,,,
            1646556,4,2025,94.0,,,,,,
            1646557,4,2025,255.0,,,,,,
            1646558,4,2025,458.0,,,,,,
            1646573,4,2025,276.077298578199,,,,,,
            1646479,4,2025,,,,,,,
            1646507,4,2025,,,,,,,
            1646508,4,2025,30461.92,,,,,,
            1646559,4,2025,,,,,,,
            1646560,4,2025,,,,,,,
            1646561,4,2025,,,,,,,
            1646562,4,2025,,,,,,,
            1646563,4,2025,,,,,,,
            1646564,4,2025,,,,,,,
            1646565,4,2025,,,,,,,
            1646566,4,2025,,,,,,,
            1646567,4,2025,,,,,,,
            1646568,4,2025,,,,,,,
            1646569,4,2025,,,,,,,
            1646570,4,2025,,,,,,,
            1646571,4,2025,,,,,,,
            1646572,4,2025,,,,,,,
        """

        df_export_meds_expected = pd.read_csv(StringIO(export_expected_csv)).set_index("CD_ID_ITEM")

        pd.testing.assert_frame_equal(df_export_meds, df_export_meds_expected)

