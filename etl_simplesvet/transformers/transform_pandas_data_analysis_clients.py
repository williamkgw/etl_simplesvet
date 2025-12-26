import pandas as pd

def enrich_clients_df(clients_df, mapping_clients_df):
    enriched_clients_df = clients_df.copy()
    enriched_clients_df = enriched_clients_df.join(mapping_clients_df, on = "TX_ORGM", how = "left")
    enriched_clients_df['TX_GRP'] = enriched_clients_df['TX_GRP'].fillna('NULL')

    return enriched_clients_df

def agg_vendas_clientes(vendas_df):
    agg_v_clientes = vendas_df \
                        .groupby([pd.Grouper(key = "TS_DT_HR_VND", freq = "1ME")])["TX_NM_CLI"] \
                        .agg(list) \
                        .pipe( \
                            lambda s: pd.Series( \
                                [len(set().union(*s.iloc[max(0, i-6): i])) for i in range(len(s))], \
                                index = s.index, \
                                name='Quantidade Totalizada Clientes Ativos' \
                            ) \
                        )

    return agg_v_clientes

def agg_clientes_mapping(clients_df):
    clientes_agrupado = clients_df.groupby([pd.Grouper(key = 'TS_DT_INCL', freq = '1ME'), 'TX_GRP'], dropna = False)
    agg = pd.DataFrame()
    agg['Quantidade Totalizada Clientes'] = clientes_agrupado.agg({'TX_ORGM': 'count'})

    agg = agg.dropna(axis = "columns", how='all')
    agg = agg.unstack(level = -1, fill_value = 0)
    return agg

def agg_clients_total(clients_df):
    clients_agrupado_tempo = clients_df.groupby([pd.Grouper(key = 'TS_DT_INCL', freq = '1ME')])
    agg_clients_total = pd.DataFrame()
    agg_clients_total['Quantidade Totalizada Clientes'] = clients_agrupado_tempo \
        .agg({ \
            'TX_ORGM': 'count' \
        })
    return agg_clients_total

