import pandas as pd

def enrich_clients_df(clients_df, mapping_clients_df):
    enriched_clients_df = clients_df.copy()
    enriched_clients_df['_grupo'] = enriched_clients_df['Origem'].map(mapping_clients_df['Grupo'])
    enriched_clients_df['_grupo'] = enriched_clients_df['_grupo'].fillna('NULL')
    return enriched_clients_df

def agg_vendas_clientes(vendas_df):
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

def agg_clientes_mapping(clients_df):
    clientes_agrupado = clients_df.groupby([pd.Grouper(key = 'Inclusão', freq = '1ME'), '_grupo'], dropna = False)
    agg = pd.DataFrame()
    agg['Quantidade Totalizada Clientes'] = clientes_agrupado.agg({'Origem': 'count'})

    agg = agg.dropna(axis = 1, how='all')
    agg = agg.unstack(level = -1, fill_value = 0)
    return agg

def agg_clients_total(clients_df):
    clients_agrupado_tempo = clients_df.groupby([pd.Grouper(key = 'Inclusão', freq = '1ME')])
    agg_clients_total = pd.DataFrame()
    agg_clients_total['Quantidade Totalizada Clientes'] = clients_agrupado_tempo \
        .agg({ \
            'Origem': 'count' \
        })
    return agg_clients_total

