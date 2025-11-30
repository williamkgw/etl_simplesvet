import pandas as pd

def enrich_clients_df(clients_df, mapping_clients_df):
    enriched_clients_df = clients_df.copy()

    mapping_clients_df = mapping_clients_df.rename(columns={"Grupo": "_grupo"})
    enriched_clients_df = enriched_clients_df.join(mapping_clients_df, on = "Origem", how = "left")
    enriched_clients_df['_grupo'] = enriched_clients_df['_grupo'].fillna('NULL')

    return enriched_clients_df

def agg_vendas_clientes(vendas_df):
    agg_v_clientes = vendas_df \
                        .groupby([pd.Grouper(key = "Data e hora", freq = "1ME")])["Cliente"] \
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

