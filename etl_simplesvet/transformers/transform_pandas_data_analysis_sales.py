import numpy as np
import pandas as pd

def enrich_sales_df(sales_df, mapping_sales_df):
    mapping_sales_columns = mapping_sales_df.columns
    sales_df = sales_df.merge(mapping_sales_df, on = "TX_PRD_SRV", how = "left")
    sales_df[mapping_sales_columns] = sales_df[mapping_sales_columns].fillna("NULL")

    # tickets
    sales_df["__ticket"] = 1 / sales_df.groupby("CD_VND")["CD_VND"].transform("count")
    sales_df["__ticket_por_pilar"] = 1 / sales_df.groupby(["CD_VND", "TX_PIL"], dropna = False)["TX_PIL"].transform("count")

    # clientes ativos
    sales_df["__clientes_ativos"] = 1 / sales_df.groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME"), "CD_CLI"], dropna = False)["CD_CLI"].transform("count")
    sales_df["__clientes_ativo_por_pilar"] = 1 / sales_df.groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME"), "CD_CLI", "TX_PIL"], dropna = False)["CD_CLI"].transform("count")

    return sales_df

def get_agg_grupo_df(vendas_agrupado_grupo):
    agg_grupo_df = vendas_agrupado_grupo[["VL_BRT", "VL_QTD"]] \
                    .agg("sum") \
                    .assign(media = lambda df: df["VL_BRT"] / df["VL_QTD"]) \
                    .rename(columns = {
                        "VL_BRT": "Faturamento Bruto",
                        "VL_QTD": "Quantidade Totalizada",
                        "media": "Preço Médio"
                        }
                    ) \
                    .unstack(level = [-2, -1]) \
                    .fillna(0)
    return agg_grupo_df

def get_agg_pilar_df(vendas_agrupado_pilar):
    agg_pilar_df = vendas_agrupado_pilar[["VL_BRT", "VL_QTD", "__ticket_por_pilar", "__clientes_ativo_por_pilar"]] \
                    .agg("sum") \
                    .assign(media = lambda df: df["VL_BRT"] / df["VL_QTD"]) \
                    .assign(media_ticket = lambda df: df["VL_BRT"] / df["__ticket_por_pilar"]) \
                    .assign(media_ticket_per_client = lambda df: df["VL_BRT"] / df["__clientes_ativo_por_pilar"]) \
                    .drop(columns = ["__ticket_por_pilar", "__clientes_ativo_por_pilar"]) \
                    .rename(columns = {
                            "VL_BRT": "Faturamento Bruto",
                            "VL_QTD": "Quantidade Totalizada",
                            "media": "Preço Médio",
                            "media_ticket": "Tickets Médio",
                            "media_ticket_per_client": "Faturamento Médio por Clientes"
                        }
                    ) \
                    .unstack(level = [-2, -1]) \
                    .fillna(0)
    return agg_pilar_df

def get_agg_categoria_df(vendas_agrupado_categoria):
    agg_categoria_df = vendas_agrupado_categoria[["VL_BRT", "VL_QTD"]] \
                        .agg("sum") \
                        .assign(media = lambda df: df["VL_BRT"] / df["VL_QTD"]) \
                        .rename(columns = {
                            "VL_BRT": "Faturamento Bruto",
                            "VL_QTD": "Quantidade Totalizada",
                            "media": "Preço Médio"
                            }
                        ) \
                        .unstack(level = -1) \
                        .fillna(0)

    return agg_categoria_df

def get_agg_tempo_df(vendas_agrupado_tempo):
    agg_tempo_df = vendas_agrupado_tempo[["VL_BRT", "VL_QTD", "__ticket", "__clientes_ativos"]] \
                    .agg("sum") \
                    .assign(media = lambda df: df["VL_BRT"] / df["VL_QTD"]) \
                    .assign(media_ticket = lambda df: df["VL_BRT"] / df["__ticket"]) \
                    .assign(media_ticket_per_client = lambda df: df["VL_BRT"] / df["__clientes_ativos"]) \
                    .drop(columns = ["__ticket", "__clientes_ativos"]) \
                    .rename(columns = {
                            "VL_BRT": "Faturamento Bruto",
                            "VL_QTD": "Quantidade Totalizada",
                            "media": "Preço Médio",
                            "media_ticket": "Tickets Médio",
                            "media_ticket_per_client": "Faturamento Médio por Clientes"
                        }
                    ) \
                    .fillna(0)

    return agg_tempo_df

def get_exception_df(vendas_agrupado_grupo):
    qntd_df = vendas_agrupado_grupo["VL_QTD"] \
                    .apply("sum") \
                    .unstack(level = [-2, -1]) \
                    .dropna(axis = "columns", how = "all")

    pilar_columns = qntd_df.columns.get_level_values(level = 0)
    grupo_columns = qntd_df.columns.get_level_values(level = 1)

    CIRURGIA = "Cirurgia"
    CONSULTA = "Consulta"
    EXAMES = "Exames"
    DIARIA = "Diária"

    if CIRURGIA in grupo_columns:
        cirurgia_s = qntd_df.xs(CIRURGIA, level = 1, axis = "columns").loc[:, "Cirurgia"]
    else:
        cirurgia_s = pd.Series(data = 0.0, index = qntd_df.index)

    if CONSULTA in grupo_columns:
        consultas_s = qntd_df.xs(CONSULTA, level = 1, axis = "columns").loc[:, "Clínica"]
    else:
        consultas_s = pd.Series(data = 0.0, index = qntd_df.index)

    if EXAMES in pilar_columns:
        exames_s = qntd_df.xs(EXAMES, level = 0, axis = "columns").sum(axis = "columns")
    else:
        exames_s = pd.Series(data = 0.0, index = qntd_df.index)

    if DIARIA in grupo_columns:
        internacao_s = qntd_df.xs(DIARIA, level = 1, axis = "columns").loc[:, "Internação"]
    else:
        internacao_s = pd.Series(data = 0.0, index = qntd_df.index)

    exception_df = pd.DataFrame(
        {
            "Consultas/Cirurgias": consultas_s/cirurgia_s,
            "Consultas/Internação": consultas_s/internacao_s,
            "Exames/Consultas": exames_s/consultas_s
        }
    ) \
    .replace([np.inf, -np.inf, np.nan], 0.0)

    return exception_df

def get_inadimplencia_df(vendas_df, end_date):
    end_date_time_mask = end_date - pd.offsets.MonthBegin()
    begin_date_time_mask = end_date - pd.offsets.DateOffset(months = 11) - 2*pd.offsets.MonthBegin()

    time_mask = (vendas_df["TS_DT_HR_VND"] > begin_date_time_mask) & (vendas_df["TS_DT_HR_VND"] < end_date_time_mask)
    baixa_mask = vendas_df["IN_STS_VND"] != "Baixado"
    mask = time_mask & baixa_mask

    inadimpl_df = vendas_df[mask] \
                    .groupby(pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME"))["VL_BRT"] \
                    .agg("sum").rolling(window = 12).sum() \
                    .rename("Inadimplencia do Faturamento Bruto")

    inadimpl_df.index = inadimpl_df.index + pd.offsets.MonthEnd()
    inadimpl_df.index.freq = "1ME"

    return inadimpl_df

