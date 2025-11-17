import pandas as pd

from etl_simplesvet.step import Step

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

def _enrich_clients_df(clients_df, mapping_clients_df):
    enriched_clients_df = clients_df.copy()
    enriched_clients_df['_grupo'] = enriched_clients_df['Origem'].map(mapping_clients_df['Grupo'])
    enriched_clients_df['_grupo'] = enriched_clients_df['_grupo'].fillna('NULL')
    return enriched_clients_df

def _agg_vendas_clientes(vendas_df):
    max_date = max(vendas_df['Data e hora'])
    end_date = pd.Period.to_timestamp(max_date.to_period(freq = '1M'))
    min_date = min(vendas_df['Data e hora'])
    start_date = pd.Period.to_timestamp(min_date.to_period(freq = '1M'))

    agg_v_clientes = pd.DataFrame()
    if start_date == end_date:
        n_clientes_6_meses = 0
        agg_new = pd.DataFrame(index = [end_date])
        agg_new['Quantidade Totalizada Clientes Ativos'] = n_clientes_6_meses
        return agg_new

    endDate = end_date
    while True:
        startDate = endDate - pd.DateOffset(months = 6)

        if startDate <= start_date:
            while True:
                startDate = start_date
                if endDate <= startDate:
                    break
                mask = (vendas_df['Data e hora'] >= startDate) & (vendas_df['Data e hora'] <= endDate)
                df = vendas_df[mask]
                n_clientes_6_meses = df['Cliente'].nunique()
                agg_new = pd.DataFrame(index = [endDate])
                agg_new['Quantidade Totalizada Clientes Ativos'] = n_clientes_6_meses
                agg_v_clientes = pd.concat([agg_new, agg_v_clientes])

                endDate -= pd.DateOffset(months = 1)
            break

        mask = (vendas_df['Data e hora'] >= startDate) & (vendas_df['Data e hora'] <= endDate)
        df = vendas_df[mask]
        n_clientes_6_meses = df['Cliente'].nunique()

        agg_new = pd.DataFrame(index = [endDate])
        agg_new['Quantidade Totalizada Clientes Ativos'] = n_clientes_6_meses
        agg_v_clientes = pd.concat([agg_new, agg_v_clientes])

        endDate -= pd.DateOffset(months = 1)

    return agg_v_clientes

def _agg_clientes_mapping(clients_df):
    clientes_agrupado = clients_df.groupby([pd.Grouper(key = 'Inclusão', freq = '1ME'), '_grupo'], dropna = False)
    agg = pd.DataFrame()
    agg['Quantidade Totalizada Clientes'] = clientes_agrupado.agg({'Origem': 'count'})

    agg = agg.dropna(axis = 1, how='all')
    agg = agg.unstack(level = -1, fill_value = 0)
    return agg

def _agg_clients_total(clients_df):
    clients_agrupado_tempo = clients_df.groupby([pd.Grouper(key = 'Inclusão', freq = '1ME')])
    agg_clients_total = pd.DataFrame()
    agg_clients_total['Quantidade Totalizada Clientes'] = clients_agrupado_tempo \
        .agg({ \
            'Origem': 'count' \
        })
    return agg_clients_total


class StepTransformPandasDataAnalysisClients(Step):
    def run(self, **kwargs):
        end_date = kwargs["end_date"]
        sales_df = kwargs["sales_df"]
        clients_df = kwargs["clients_df"]
        mapping_sales_df = kwargs["mapping_sales_df"]
        mapping_clients_df = kwargs["mapping_clients_df"]

        mapping_clients_df, *_  = _treat_mapping_clients(mapping_clients_df)

        clients_df = _enrich_clients_df(clients_df, mapping_clients_df)

        agg_clientes = _agg_clientes_mapping(clients_df)
        agg_clientes = agg_clientes.loc[agg_clientes.index[-6:]]

        agg_clientes_total = _agg_clients_total(clients_df)
        agg_clientes_total = agg_clientes_total.loc[agg_clientes_total.index[-6:]]

        agg_v_clientes = _agg_vendas_clientes(sales_df)
        agg_v_clientes = agg_v_clientes.loc[agg_v_clientes.index[-6:]]

        return {
                "agg_clientes": agg_clientes,
                "agg_clientes_total": agg_clientes_total,
                "agg_v_clientes": agg_v_clientes,
                "clients_df": clients_df,
                **kwargs
        }
