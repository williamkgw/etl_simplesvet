import pandas as pd
from etl_simplesvet.ingesters.ingester_pandas_sql import IngesterPandasSQL

from etl_simplesvet.step import Step

class StepIngestPandasExport(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]

        export_template_query = """
            SELECT *
            FROM EXPT_TMPL
            WHERE TX_COMP = 'Empresa'
        """
        export_df = IngesterPandasSQL(export_template_query).ingest()
        export_df = export_df \
                    .set_index("CD_ID_ITEM") \
                    .drop("TX_COMP", axis = "columns")

        export_df["VL_MES"] = end_date.month - 1
        export_df["VL_ANO"] = end_date.year

        agg_vendas_grupo_df = kwargs["agg_grupo_df"]
        agg_vendas_pil_df = kwargs["agg_pilar_df"]
        agg_vendas_cat_df = kwargs["agg_categoria_df"]
        agg_vendas_tot_df = kwargs["agg_tempo_df"]
        agg_inadimplencia_df = pd.DataFrame(kwargs["inadimplencia"])
        agg_vendas_exec_df = kwargs["agg_exception_df"]

        mapping_item_query = """
            SELECT *
            FROM MAP_ITEM
            WHERE TX_COMP = 'Empresa'
        """

        mapping_item_df = IngesterPandasSQL(mapping_item_query).ingest()
        mapping_item_df = mapping_item_df \
                            .set_index("CD_ID_ITEM") \
                            .drop("TX_COMP", axis = "columns")

        agg_clientes_grupo_df = kwargs["agg_clientes"]
        agg_clientes_total_df = kwargs["agg_clientes_total"]
        agg_clientes_total_ativos_df = pd.DataFrame(kwargs["agg_v_clientes"])

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

