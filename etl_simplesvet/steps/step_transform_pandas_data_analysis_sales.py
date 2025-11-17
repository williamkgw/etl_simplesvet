from etl_simplesvet.step import Step

from etl_simplesvet.transformers.transform_pandas_data_analysis_sales import (
    test_vendas,
    test_inadimplente
)

def test_mapping_vendas(mapping_vendas_df):
    # removing empty rows
    missing_mapping_vendas_df = mapping_vendas_df[mapping_vendas_df.isna().all(axis=1)]
    mapping_vendas_df = mapping_vendas_df.dropna(how = 'all', axis = 0)

    # configuring the dataframes to catch case sensitive
    mapping_vendas_df.index = mapping_vendas_df.index.str.lower()

    # removing duplicated index
    mapping_vendas_duplicated_df = mapping_vendas_df[mapping_vendas_df.index.duplicated(keep = False)]
    mapping_vendas_df = mapping_vendas_df[~mapping_vendas_df.index.duplicated(keep='last')]

    return [mapping_vendas_df, mapping_vendas_duplicated_df, missing_mapping_vendas_df]


class  StepTransformPandasDataAnalysisSales(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]
        sales_df = kwargs["sales_df"]
        clients_df = kwargs["clients_df"]
        mapping_sales_df = kwargs["mapping_sales_df"]
        mapping_clients_df = kwargs["mapping_clients_df"]

        mapping_sales_df, *_ = test_mapping_vendas(mapping_sales_df)

        result_vendas = test_vendas(sales_df, mapping_sales_df)
        inadimplencia = test_inadimplente(sales_df, end_date)

        return {
            **result_vendas,
            "mapping_sales_df": mapping_sales_df,
            "inadimplencia": inadimplencia,
            "clients_df": clients_df,
            "mapping_clients_df": mapping_clients_df,
            "end_date": end_date,
            **kwargs
        }
