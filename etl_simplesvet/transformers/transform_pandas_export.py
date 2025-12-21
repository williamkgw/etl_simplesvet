import pandas as pd
import numpy as np

from etl_simplesvet.ingesters.ingester_pandas_mapping_export import IngesterPandasMappingExport
from etl_simplesvet.ingesters.ingester_pandas_export import IngesterPandasExport

def reset_export_df(export_df, end_date):
    export_df = export_df.copy()
    export_df.loc[:, "Mês"] = end_date.month - 1
    export_df.loc[:, "Ano"] = end_date.year
    cols_to_reset = (
        "Medição",
        "Fx Verde Inf/Previsto",
        "Fx Verde Sup",
        "Fx Vermelha Inf",
        "Fx Vermelha Sup",
        "Fx Cliente Inf",
        "Fx Cliente Sup"
    )
    export_df.loc[:, cols_to_reset] = np.nan
    return export_df

from enum import StrEnum

class DateColumns(StrEnum):
    SALES_DATE = "Data e hora"
    CLIENTS_DATE = "Inclusão"
    DEFAULT_DATE = "Data e hora"


def get_date_med_op_df(df):
    df_flat = df.unstack().reset_index().rename(columns = {0: "med"})

    if DateColumns.SALES_DATE in df_flat.columns:
        date_col = DateColumns.SALES_DATE
    elif DateColumns.CLIENTS_DATE in df_flat.columns:
        date_col = DateColumns.CLIENTS_DATE
    else:
        date_col = DateColumns.DEFAULT_DATE

    return df_flat \
            .assign(op = lambda df: df.loc[:, :date_col].iloc[:, :-1].agg(tuple, axis = "columns")) \
            .loc[:, date_col:] \
            .rename(columns = {date_col: DateColumns.DEFAULT_DATE})

def get_meds_df(export_sales_frames, export_clients_frames):
    agg_dfs = [
        export_sales_frames.agg_vendas_grupo_df,
        export_sales_frames.agg_vendas_pil_df,
        export_sales_frames.agg_vendas_cat_df,
        export_sales_frames.agg_vendas_tot_df,
        export_sales_frames.agg_inadimplencia_df,
        export_sales_frames.agg_vendas_exec_df,
        export_clients_frames.agg_clientes_grupo_df,
        export_clients_frames.agg_clientes_total_df,
        export_clients_frames.agg_clientes_total_ativos_df
    ]
    treated_dfs = map(get_date_med_op_df, agg_dfs)
    meds_df = pd.concat(list(treated_dfs))
    return meds_df


def med(export_df, meds_df, mapping_item_df, end_date):
    last_med_df = meds_df[meds_df["Data e hora"] == max(meds_df["Data e hora"])].copy()
    last_med_df["Ano"] = last_med_df["Data e hora"].dt.year
    last_med_df["Mês"] = last_med_df["Data e hora"].dt.month
    last_med_df = last_med_df.drop("Data e hora", axis = "columns")
    last_med_df = last_med_df.drop_duplicates()

    OP_COLUMNS = ["Op_execao", "Op", "Categoria", "Pilar", "Grupo"]
    mapping_item_df["op"] = mapping_item_df[OP_COLUMNS] \
                            .agg(tuple, axis = "columns") \
                            .transform(lambda s: tuple(filter(lambda c: c != 'x', s)))

    export_df = reset_export_df(export_df, end_date)
    export_med_data_df = mapping_item_df \
                        .drop(["Ano", "Mês"], axis = "columns") \
                        .reset_index() \
                        .merge(last_med_df, on = "op") \
                        .loc[:, ["ID do Item", "Ano", "Mês", "med"]] \
                        .set_index("ID do Item")
    export_df = export_df \
                .drop("Medição", axis = "columns") \
                .merge(export_med_data_df, on = ["ID do Item", "Ano", "Mês"], how = "left") \
                .rename(columns = {"med": "Medição"}) \
                .loc[:, export_df.columns]

    return export_df

