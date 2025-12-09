from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from etl_simplesvet.step import Step

from etl_simplesvet.transformers.transform_pandas_export import (
    med
)


@dataclass
class ExportSalesFrames:
    agg_vendas_grupo_df: pd.DataFrame
    agg_vendas_pil_df: pd.DataFrame
    agg_vendas_cat_df: pd.DataFrame
    agg_vendas_tot_df: pd.DataFrame
    agg_inadimplencia_df: pd.DataFrame
    agg_vendas_exec_df: pd.DataFrame


@dataclass
class ExportClientsFrames:
    agg_clientes_grupo_df: pd.DataFrame
    agg_clientes_total_df: pd.DataFrame
    agg_clientes_total_ativos_df: pd.DataFrame


class  StepTransformPandasExport(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]
        export_df = kwargs["export_df"]
        mapping_item_df = kwargs["mapping_item_df"]

        export_sales_frames_ctx = {
            k: v for k, v in kwargs.items() if k in [
                "agg_vendas_grupo_df",
                 "agg_vendas_pil_df",
                 "agg_vendas_cat_df",
                 "agg_vendas_tot_df",
                 "agg_inadimplencia_df",
                 "agg_vendas_exec_df"
            ]
        }
        export_sales_frames = ExportSalesFrames(**export_sales_frames_ctx)

        export_clients_frames_ctx = {
            k: v for k, v in kwargs.items() if k in [
            "agg_clientes_grupo_df",
            "agg_clientes_total_df",
            "agg_clientes_total_ativos_df"
            ]
        }
        export_clients_frames = ExportClientsFrames(**export_clients_frames_ctx)

        df_export = med(
            export_df,
            export_sales_frames,
            export_clients_frames,
            mapping_item_df,
            end_date,
            -1
        )

        return {
            **kwargs,
            "df_export": df_export
        }
