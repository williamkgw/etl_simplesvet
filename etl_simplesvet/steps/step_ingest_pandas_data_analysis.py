from datetime import datetime

from etl_simplesvet.ingesters.ingester_pandas_sales import IngesterPandasSales
from etl_simplesvet.ingesters.ingester_pandas_clients import IngesterPandasClients
from etl_simplesvet.ingesters.ingester_pandas_sql import IngesterPandasSQL

from etl_simplesvet.step import Step

class StepIngestPandasDataAnalysis(Step):
    def run(self, **kwargs):

        end_date = datetime.strptime("2025-05-01", "%Y-%m-%d")
        sales_df = IngesterPandasSales("datasets/Vendas.csv", end_date).ingest()
        clients_df = IngesterPandasClients("datasets/Clientes.csv", end_date).ingest()

        mapping_sales_query = """
            SELECT *
            FROM MAP_SLS
            WHERE TX_COMP = 'Empresa'
        """
        mapping_sales_df = IngesterPandasSQL(mapping_sales_query).ingest()
        mapping_sales_df = mapping_sales_df.set_index("TX_PRD_SRV")
        mapping_sales_df = mapping_sales_df.drop("TX_COMP", axis = "columns")

        mapping_clients_query = """
            SELECT *
            FROM MAP_CLNT
            WHERE TX_COMP = 'Empresa'
        """
        mapping_clients_df = IngesterPandasSQL(mapping_clients_query).ingest()
        mapping_clients_df = mapping_clients_df.set_index("TX_ORGM")
        mapping_clients_df = mapping_clients_df.drop("TX_COMP", axis = "columns")

        return {
            **kwargs,
            "end_date": end_date,
            "sales_df": sales_df,
            "clients_df": clients_df,
            "mapping_sales_df": mapping_sales_df,
            "mapping_clients_df": mapping_clients_df
        }

