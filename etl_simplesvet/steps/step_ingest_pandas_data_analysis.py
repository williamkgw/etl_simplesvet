from etl_simplesvet.ingesters.ingester_pandas_sales import IngesterPandasSales
from etl_simplesvet.ingesters.ingester_pandas_clients import IngesterPandasClients
from etl_simplesvet.ingesters.ingester_pandas_mapping import IngesterPandasMapping
from etl_simplesvet.ingesters.ingester_pandas_mapping_clients import IngesterPandasMappingClients

from etl_simplesvet.step import Step

class StepPandasDataAnalysis(Step):
    def run(self, **kwargs):

        sales_df = IngesterPandasSales("datasets/Vendas.csv", "2023-01-01").ingest()
        clients_df = IngesterPandasClients("datasets/Clientes.csv", "2023-01-01").ingest()
        mapping_sales_df = IngesterPandasMapping("datasets/new_mapping.xlsx").ingest()
        mapping_clients_df = IngesterPandasMappingClients("datasets/new_mapping_cliente.xlsx").ingest()

        return {
                    "sales_df": sales_df,
                    "clients_df": clients_df,
                    "mapping_sales_df": mapping_sales_df,
                    "mapping_clients_df": mapping_clients_df
                }
