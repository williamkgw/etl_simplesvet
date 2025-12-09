from etl_simplesvet.ingesters.ingester_pandas_export import IngesterPandasExport
from etl_simplesvet.ingesters.ingester_pandas_mapping_export import IngesterPandasMappingExport
from etl_simplesvet.ingesters.ingester_pandas_xlsx import IngesterPandasXLSX

from etl_simplesvet.step import Step

class StepIngestPandasExport(Step):
    def run(self, **kwargs):
        AGG_VENDAS_FILE = "datasets/output/test_agg_1.xlsx"
        AGG_CLIENTES_FILE = "datasets/output/test_agg_clientes_1.xlsx"
        MAPPING_EXPORT_FILE = "datasets/mapping_item.xlsx"

        arithmetic_seq_list = lambda i : list(range(i))

        export_df = IngesterPandasExport("datasets/import.xlsx").ingest()
        ingester_xlsx = IngesterPandasXLSX(AGG_VENDAS_FILE)

        agg_vendas_grupo_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(3), sheet_name = "grupo").ingest()
        agg_vendas_pil_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(3), sheet_name = "pilar").ingest()
        agg_vendas_cat_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(2), sheet_name = "categoria").ingest()
        agg_vendas_tot_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(1), sheet_name = "total").ingest()
        agg_inadimplencia_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(1), sheet_name = "inadimplencia").ingest()
        agg_vendas_exec_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(1), sheet_name = "exception").ingest()

        mapping_item_df = IngesterPandasMappingExport(MAPPING_EXPORT_FILE).ingest()

        ingester_xlsx.pass_options(io=AGG_CLIENTES_FILE)
        agg_clientes_grupo_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(2), sheet_name = "grupo_clientes").ingest()
        agg_clientes_total_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(1), sheet_name = "grupo_total").ingest()
        agg_clientes_total_ativos_df = ingester_xlsx.pass_options(header = arithmetic_seq_list(1), sheet_name = "ativos_clientes").ingest()

        return {
            **kwargs,
            "export_df": export_df,
            "agg_vendas_grupo_df": agg_vendas_grupo_df,
            "agg_vendas_pil_df": agg_vendas_pil_df,
            "agg_vendas_cat_df": agg_vendas_cat_df,
            "agg_vendas_tot_df": agg_vendas_tot_df,
            "agg_inadimplencia_df": agg_inadimplencia_df,
            "agg_vendas_exec_df": agg_vendas_exec_df,
            "mapping_item_df": mapping_item_df,
            "agg_clientes_grupo_df": agg_clientes_grupo_df,
            "agg_clientes_total_df": agg_clientes_total_df,
            "agg_clientes_total_ativos_df": agg_clientes_total_ativos_df
        }

