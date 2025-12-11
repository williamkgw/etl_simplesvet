import pandas as pd
import numpy as np

from etl_simplesvet.ingesters.ingester_pandas_mapping_export import IngesterPandasMappingExport
from etl_simplesvet.ingesters.ingester_pandas_export import IngesterPandasExport

def med_n_levels(import_df, agg, mapping_item_df, mapping_item_cols, n):
    for index, row in mapping_item_df.iterrows():
        multiplicador = row["Multiplicador"]

        if len(mapping_item_cols) != 1:
            column_or_columns = tuple(row[mapping_item_cols])
        else:
            column_or_columns = row[mapping_item_cols].iloc[0]

        medicao = agg.get(column_or_columns, default = pd.DataFrame([0])).iloc[n]
        import_df.loc[index, "Medição"] = multiplicador*medicao

    return import_df

def filter_mapping_item_df(mapping_item_df, type_of_filtering):
    type_of_filtering = type_of_filtering.casefold()
    args = ["Categoria", "Pilar", "Grupo", "Op", "Op_execao"]

    has_x       = lambda column: mapping_item_df[column] == "x"
    has_total   = lambda column: mapping_item_df[column].str.casefold() == "total"
    has_clientes = lambda: mapping_item_df["Op"] == "Quantidade Totalizada Clientes"
    has_clientes_ativos = lambda: mapping_item_df["Op"] == "Quantidade Totalizada Clientes Ativos"
    has_inadimplencia = lambda: mapping_item_df["Op_execao"] == "Inadimplencia do Faturamento Bruto"

    if type_of_filtering == "grupo_cliente":
        mask = has_x(args[0]) & has_x(args[1]) & ~has_total(args[2]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & has_clientes() & ~has_clientes_ativos()

    elif type_of_filtering == "grupo_total":
        mask = has_x(args[0]) & has_x(args[1]) & has_total(args[2]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & has_clientes() & ~has_clientes_ativos()

    elif type_of_filtering == "total_cliente":
        mask = has_x(args[0]) & has_x(args[1]) & has_total(args[2]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & ~has_clientes() & has_clientes_ativos()

    elif type_of_filtering == "grupo":
        mask = has_x(args[0]) & ~has_x(args[1]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & ~has_clientes() & ~has_clientes_ativos()

    elif type_of_filtering == "pilar":
        mask = ~has_x(args[0]) & ~has_x(args[1]) & has_x(args[2]) & ~has_x(args[3]) & has_x(args[4])

    elif type_of_filtering == "categoria":
        mask = ~has_total(args[0]) & ~has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & ~has_x(args[3]) & has_x(args[4])

    elif type_of_filtering == "total":
        mask = has_total(args[0]) & ~has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & ~has_x(args[3]) & has_x(args[4])

    elif type_of_filtering == "exception":
        mask = has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & has_x(args[3]) & ~has_x(args[4]) & ~has_inadimplencia()

    elif type_of_filtering == "inadimplencia":
        mask = has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & has_x(args[3]) & ~has_x(args[4]) & has_inadimplencia()

    else:
        return pd.DataFrame(columns = mapping_item_df.columns)

    return mapping_item_df[mask]

def med_base(
    import_df,
    agg_vendas_df,
    mapping_item_df,
    n, mapping_item_cols,
    med_type
 ):
    mapping_item_df = filter_mapping_item_df(mapping_item_df, med_type)
    export_df = med_n_levels(
        import_df,
        agg_vendas_df,
        mapping_item_df,
        mapping_item_cols,
        n
    )
    return export_df

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
    export_df.loc[:, cols_to_reset] = pd.NA
    return export_df

def med(
        export_df,
        export_sales_frames,
        export_clients_frames,
        mapping_item_df,
        end_date,
        n
):
    export_df = reset_export_df(export_df, end_date)

    med_ops_ctx = [
        {
            "mapping_item_cols": ["Op", "Pilar", "Grupo"],
            "med_type": "grupo",
            "agg": export_sales_frames.agg_vendas_grupo_df
        },
        {
            "mapping_item_cols": ["Op", "Categoria", "Pilar"],
            "med_type": "pilar",
            "agg": export_sales_frames.agg_vendas_pil_df
        },
        {
            "mapping_item_cols": ["Op", "Categoria"],
            "med_type": "categoria",
            "agg": export_sales_frames.agg_vendas_cat_df
        },
        {
            "mapping_item_cols": ["Op"],
            "med_type": "total",
            "agg": export_sales_frames.agg_vendas_tot_df
        },
        {
            "mapping_item_cols": ["Op_execao"],
            "med_type": "inadimplencia",
            "agg": export_sales_frames.agg_inadimplencia_df
        },
        {
            "mapping_item_cols": ["Op_execao"],
            "med_type": "exception",
            "agg": export_sales_frames.agg_vendas_exec_df
        },
        {
            "mapping_item_cols": ["Op", "Grupo"],
            "med_type": "grupo_cliente",
            "agg": export_clients_frames.agg_clientes_grupo_df
        },
        {
            "mapping_item_cols": ["Op"],
            "med_type": "grupo_total",
            "agg": export_clients_frames.agg_clientes_total_df
        },
        {
            "mapping_item_cols": ["Op"],
            "med_type": "total_cliente",
            "agg": export_clients_frames.agg_clientes_total_ativos_df
        }
    ]

    for med_op_ctx in med_ops_ctx:
        mapping_item_cols = med_op_ctx["mapping_item_cols"]
        med_type = med_op_ctx["med_type"]
        agg = med_op_ctx["agg"]
        export_df = med_base(export_df, agg, mapping_item_df, n, mapping_item_cols, med_type)

    export_df = export_df.dropna(subset = ("Mês", "Ano"))
    export_df = export_df.replace([np.inf, -np.inf], 0)

    return export_df

