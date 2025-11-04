from datetime import datetime

from etl_simplesvet.ingesters.ingester_pandas_sales import IngesterPandasSales
from etl_simplesvet.ingesters.ingester_pandas_clients import IngesterPandasClients
from etl_simplesvet.ingesters.ingester_pandas_mapping import IngesterPandasMapping
from etl_simplesvet.ingesters.ingester_pandas_mapping_clients import IngesterPandasMappingClients

from etl_simplesvet.step import Step

class StepIngestPandasDataAnalysis(Step):
    def run(self, **kwargs):

        end_date = datetime.strptime("2026-01-01", "%Y-%m-%d")
        sales_df = IngesterPandasSales("datasets/Vendas.csv", end_date).ingest()
        clients_df = IngesterPandasClients("datasets/Clientes.csv", end_date).ingest()
        mapping_sales_df = IngesterPandasMapping("datasets/new_mapping.xlsx").ingest()
        mapping_clients_df = IngesterPandasMappingClients("datasets/new_mapping_cliente.xlsx").ingest()

        return {
            "end_date": end_date,
            "sales_df": sales_df,
            "clients_df": clients_df,
            "mapping_sales_df": mapping_sales_df,
            "mapping_clients_df": mapping_clients_df
        }
