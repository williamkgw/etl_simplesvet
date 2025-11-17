from etl_simplesvet.step import Step

from etl_simplesvet.transformers.transform_pandas_data_analysis_clients import (
    enrich_clients_df,
    agg_clientes_mapping,
    agg_clients_total,
    agg_vendas_clientes
)

def _treat_mapping_clients(mapping_clients_df):
    # removing empty rows
    missing_mapping_clients_df = mapping_clients_df[mapping_clients_df.isna().all(axis=1)]
    mapping_clients_df = mapping_clients_df.dropna(how = 'all', axis = 0)

    # configuring the dataframes to catch case sensitive
    mapping_clients_df.index = mapping_clients_df.index.str.lower()

    # removing duplicated index
    mapping_clientes_duplicated_df = mapping_clients_df[mapping_clients_df.index.duplicated(keep = False)]
    mapping_clients_df = mapping_clients_df[~mapping_clients_df.index.duplicated(keep='last')]

    return [mapping_clients_df, mapping_clientes_duplicated_df, missing_mapping_clients_df]


class StepTransformPandasDataAnalysisClients(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]
        sales_df = kwargs["sales_df"]
        clients_df = kwargs["clients_df"]
        mapping_sales_df = kwargs["mapping_sales_df"]
        mapping_clients_df = kwargs["mapping_clients_df"]

        mapping_clients_df, *_  = _treat_mapping_clients(mapping_clients_df)

        clients_df = enrich_clients_df(clients_df, mapping_clients_df)

        agg_clientes = agg_clientes_mapping(clients_df)
        agg_clientes = agg_clientes.loc[agg_clientes.index[-6:]]

        agg_clientes_total = agg_clients_total(clients_df)
        agg_clientes_total = agg_clientes_total.loc[agg_clientes_total.index[-6:]]

        agg_v_clientes = agg_vendas_clientes(sales_df)
        agg_v_clientes = agg_v_clientes.loc[agg_v_clientes.index[-6:]]

        return {
                "agg_clientes": agg_clientes,
                "agg_clientes_total": agg_clientes_total,
                "agg_v_clientes": agg_v_clientes,
                "clients_df": clients_df,
                **kwargs
        }
