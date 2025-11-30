from etl_simplesvet.step import Step

from etl_simplesvet.transformers.transform_pandas_data_analysis_clients import (
    enrich_clients_df,
    agg_clientes_mapping,
    agg_clients_total,
    agg_vendas_clientes
)


class StepTransformPandasDataAnalysisClients(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]
        sales_df = kwargs["sales_df"]
        clients_df = kwargs["clients_df"]
        mapping_sales_df = kwargs["mapping_sales_df"]
        mapping_clients_df = kwargs["mapping_clients_df"]

        clients_df = enrich_clients_df(clients_df, mapping_clients_df)
        agg_clientes = agg_clientes_mapping(clients_df)
        agg_clientes_total = agg_clients_total(clients_df)
        agg_v_clientes = agg_vendas_clientes(sales_df)

        return {
                **kwargs,
                "clients_df": clients_df,
                "agg_clientes": agg_clientes.loc[agg_clientes.index[-6:]],
                "agg_clientes_total": agg_clientes_total.loc[agg_clientes_total.index[-6:]],
                "agg_v_clientes": agg_v_clientes.loc[agg_v_clientes.index[-6:]]
        }
