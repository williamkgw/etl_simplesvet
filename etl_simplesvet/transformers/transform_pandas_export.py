import pandas as pd
import numpy as np

from etl_simplesvet.ingesters.ingester_pandas_mapping_export import IngesterPandasMappingExport
from etl_simplesvet.ingesters.ingester_pandas_export import IngesterPandasExport

def med_n_levels(import_df, agg_vendas_df, mapping_item_df, mapping_item_cols, n):
    id_item_col = "ID do Item"
    constant_col = "Multiplicador"

    numero_niveis = len(mapping_item_cols)
    header_list = list(range(numero_niveis))
    for index, row in mapping_item_df.iterrows():

        if type(mapping_item_cols) is list:
            columns       = row[mapping_item_cols].to_list()
            tuple_columns = tuple(columns)
            med_s = agg_vendas_df.get(tuple_columns, default = 0)
            if type(med_s) == int:
                med = med_s
            else:
                med = med_s.iloc[n]
        else:
            column = row[mapping_item_cols]
            med_s = agg_vendas_df.get(column, default = 0)
            if type(med_s) == int:
                med = med_s
            else:
                med = med_s.iloc[n]

        k = row[constant_col]
        import_df.loc[index, "Medição"] = k*med

    return import_df

def filter_mapping_item_df(mapping_item_df, type_of_filtering):

    args = ["Categoria", "Pilar", "Grupo", "Op", "Op_execao"]

    null_mapping_item_df = pd.DataFrame(columns = mapping_item_df.columns)

    has_x       = lambda column: mapping_item_df[column] == "x"
    has_total   = lambda column: mapping_item_df[column].str.casefold() == "total"
    has_clientes = lambda: mapping_item_df["Op"] == "Quantidade Totalizada Clientes"
    has_clientes_ativos = lambda: mapping_item_df["Op"] == "Quantidade Totalizada Clientes Ativos"
    has_inadimplencia = lambda: mapping_item_df["Op_execao"] == "Inadimplencia do Faturamento Bruto"

    if type_of_filtering.casefold() == "grupo_cliente":
        mask = has_x(args[0]) & has_x(args[1]) & ~has_total(args[2]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & has_clientes() & ~has_clientes_ativos()
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "grupo_total":
        mask = has_x(args[0]) & has_x(args[1]) & has_total(args[2]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & has_clientes() & ~has_clientes_ativos()
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "total_cliente":
        mask = has_x(args[0]) & has_x(args[1]) & has_total(args[2]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & ~has_clientes() & has_clientes_ativos()
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "grupo":

        mask = has_x(args[0]) & ~has_x(args[1]) & ~has_x(args[2]) & ~has_x(args[3]) & has_x(args[4]) & ~has_clientes() & ~has_clientes_ativos()
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "pilar":

        mask = ~has_x(args[0]) & ~has_x(args[1]) & has_x(args[2]) & ~has_x(args[3]) & has_x(args[4])
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "categoria":

        mask = ~has_total(args[0]) & ~has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & ~has_x(args[3]) & has_x(args[4])
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "total":

        mask = has_total(args[0]) & ~has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & ~has_x(args[3]) & has_x(args[4])
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "exception":

        mask = has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & has_x(args[3]) & ~has_x(args[4]) & ~has_inadimplencia()
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    elif type_of_filtering.casefold() == "inadimplencia":

        mask = has_x(args[0]) & has_x(args[1]) & has_x(args[2]) & has_x(args[3]) & ~has_x(args[4]) & has_inadimplencia()
        mapping_item_df = mapping_item_df[mask]
        return mapping_item_df

    return null_mapping_item_df

def med_grupo(import_df, agg_vendas_df, mapping_item_df, n):
    mapping_item_cols = ["Op", "Pilar", "Grupo"]
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "grupo")
    import_df = med_n_levels(import_df, agg_vendas_df,
                                mapping_item_df, mapping_item_cols, n)

    return import_df

def med_pilar(import_df, agg_vendas_df, mapping_item_df, n):
    mapping_item_cols = ["Op", "Categoria", "Pilar"]
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "pilar")
    import_df = med_n_levels(import_df, agg_vendas_df,
                                mapping_item_df, mapping_item_cols, n)

    return import_df

def med_categoria(import_df, agg_vendas_df, mapping_item_df, n):
    mapping_item_cols = ["Op", "Categoria"]
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "categoria")
    import_df = med_n_levels(import_df, agg_vendas_df,
                                mapping_item_df, mapping_item_cols, n)

    return import_df

def med_total(import_df, agg_vendas_df, mapping_item_df, n):
    mapping_item_cols = "Op"
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "total")
    import_df = med_n_levels(import_df, agg_vendas_df,
                                mapping_item_df, mapping_item_cols, n)

    return import_df

def med_execao(import_df, agg_vendas_df, mapping_item_df, n):
    mapping_item_cols = "Op_execao"
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "exception")
    import_df = med_n_levels(import_df, agg_vendas_df,
                                mapping_item_df, mapping_item_cols, n)

    return import_df

def med_inadimplencia_df(import_df, agg_inadimplencia_df, mapping_item_df, n):
    mapping_item_cols = "Op_execao"
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "inadimplencia")
    import_df = med_n_levels(import_df, agg_inadimplencia_df,
                                mapping_item_df, mapping_item_cols, n)

    return import_df

def med_clientes_grupo(import_df, agg_clientes_df, mapping_item_df, n):
    mapping_item_cols = ["Op", "Grupo"]
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "grupo_cliente")
    import_df = med_n_levels(import_df, agg_clientes_df, mapping_item_df, mapping_item_cols, n)
    return import_df

def med_clientes_total(import_df, agg_clientes_df, mapping_item_df, n):
    mapping_item_cols = "Op"
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "grupo_total")
    import_df = med_n_levels(import_df, agg_clientes_df, mapping_item_df, mapping_item_cols, n)
    return import_df

def med_clientes_total_ativos(import_df, agg_clientes_total_df, mapping_item_df, n):
    mapping_item_cols = "Op"
    mapping_item_df = filter_mapping_item_df(mapping_item_df, "total_cliente")
    import_df = med_n_levels(import_df, agg_clientes_total_df, mapping_item_df, mapping_item_cols, n)
    return import_df

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

    export_df = med_grupo(export_df, export_sales_frames.agg_vendas_grupo_df, mapping_item_df, n)
    export_df = med_pilar(export_df, export_sales_frames.agg_vendas_pil_df, mapping_item_df, n)
    export_df = med_categoria(export_df, export_sales_frames.agg_vendas_cat_df, mapping_item_df, n)
    export_df = med_total(export_df, export_sales_frames.agg_vendas_tot_df, mapping_item_df, n)
    export_df = med_inadimplencia_df(export_df, export_sales_frames.agg_inadimplencia_df, mapping_item_df, n)
    export_df = med_execao(export_df, export_sales_frames.agg_vendas_exec_df, mapping_item_df, n)

    export_df = med_clientes_grupo(export_df, export_clients_frames.agg_clientes_grupo_df, mapping_item_df, n)
    export_df = med_clientes_total(export_df, export_clients_frames.agg_clientes_total_df, mapping_item_df, n)
    export_df = med_clientes_total_ativos(export_df, export_clients_frames.agg_clientes_total_ativos_df, mapping_item_df, n)

    export_df = export_df.dropna(subset = ("Mês", "Ano"))
    export_df = export_df.replace([np.inf, -np.inf], 0)

    return export_df

