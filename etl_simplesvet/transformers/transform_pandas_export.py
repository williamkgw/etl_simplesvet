import pandas as pd

from enum import StrEnum

class DateColumns(StrEnum):
    SALES_DATE = "TS_DT_HR_VND"
    CLIENTS_DATE = "TS_DT_INCL"
    DEFAULT_DATE = "TS_DT_HR_VND"


def get_date_med_op_df(df):
    df_flat = df.unstack().reset_index().rename(columns = {0: "VL_MED"})

    if DateColumns.SALES_DATE in df_flat.columns:
        date_col = DateColumns.SALES_DATE
    elif DateColumns.CLIENTS_DATE in df_flat.columns:
        date_col = DateColumns.CLIENTS_DATE
    else:
        date_col = DateColumns.DEFAULT_DATE

    return df_flat \
            .assign(TX_OPS = lambda df: df.loc[:, :date_col].iloc[:, :-1].agg(tuple, axis = "columns")) \
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
    last_med_df = meds_df[meds_df["TS_DT_HR_VND"] == max(meds_df["TS_DT_HR_VND"])].copy()
    last_med_df = last_med_df.drop_duplicates()

    OP_COLUMNS = ["TX_OP_EXCE", "TX_OP", "TX_CAT", "TX_PIL", "TX_GRP"]
    mapping_item_df["TX_OPS"] = mapping_item_df[OP_COLUMNS] \
                            .agg(tuple, axis = "columns") \
                            .transform(lambda s: tuple(filter(lambda c: c != 'x', s)))

    export_med_data_df = mapping_item_df \
                        .reset_index() \
                        .merge(last_med_df, on = "TX_OPS")

    export_med_data_df["VL_ANO"] = export_med_data_df["TS_DT_HR_VND"].dt.year
    export_med_data_df["VL_MES"] = export_med_data_df["TS_DT_HR_VND"].dt.month

    export_df = export_df \
                .reset_index() \
                .drop("VL_MED", axis = "columns") \
                .merge(export_med_data_df, on = ["CD_ID_ITEM", "VL_ANO", "VL_MES"], how = "left") \
                .set_index("CD_ID_ITEM") \
                .loc[:, export_df.columns]

    return export_df

