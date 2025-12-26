import pandas as pd

from etl_simplesvet.step import Step

from etl_simplesvet.transformers.transform_pandas_data_analysis_sales import (
    enrich_sales_df,
    get_agg_grupo_df,
    get_agg_pilar_df,
    get_agg_categoria_df,
    get_agg_tempo_df,
    get_exception_df,
    get_inadimplencia_df
)


class  StepTransformPandasDataAnalysisSales(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]
        sales_df = kwargs["sales_df"]
        clients_df = kwargs["clients_df"]
        mapping_sales_df = kwargs["mapping_sales_df"]
        mapping_clients_df = kwargs["mapping_clients_df"]

        # enrich sales
        sales_df = enrich_sales_df(sales_df, mapping_sales_df)

        # sales output diagnosis
        sales_missing_df = sales_df[sales_df[["TX_PIL", "TX_GRP"]].isna().any(axis = "columns")]

        # grouped sales
        sales_groupedby_grupo = sales_df.groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME"), "TX_PIL" ,"TX_GRP"], dropna = False)
        sales_groupedby_pilar = sales_df.groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME"), "TX_CAT" ,"TX_PIL"], dropna = False)
        sales_groupedby_categoria = sales_df.groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME"), "TX_CAT"], dropna = False)
        sales_groupedby_tempo = sales_df \
                                .dropna(subset = ["TX_CAT", "TX_PIL"], how = "all") \
                                .groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME")])

        agg_grupo_df = get_agg_grupo_df(sales_groupedby_grupo)
        agg_pilar_df = get_agg_pilar_df(sales_groupedby_pilar)
        agg_categoria_df = get_agg_categoria_df(sales_groupedby_categoria)
        agg_tempo_df = get_agg_tempo_df(sales_groupedby_tempo)
        exception_df = get_exception_df(sales_groupedby_grupo)
        unique_mapping_df = mapping_sales_df.groupby(["TX_CAT", "TX_PIL", "TX_GRP"]).size()
        inadimplencia_df = get_inadimplencia_df(sales_df, end_date)
        return {
            **kwargs,
            "agg_grupo_df": agg_grupo_df.loc[agg_grupo_df.index[-6:]],
            "agg_pilar_df": agg_pilar_df.loc[agg_pilar_df.index[-6:]],
            "agg_categoria_df": agg_categoria_df.loc[agg_categoria_df.index[-6:]],
            "agg_tempo_df": agg_tempo_df.loc[agg_tempo_df.index[-6:]],
            "agg_exception_df": exception_df.loc[exception_df.index[-6:]],
            "unique_mapping_df": unique_mapping_df,
            "vendas_missing_df": sales_missing_df,
            "sales_df": sales_df,
            "inadimplencia": inadimplencia_df
        }
