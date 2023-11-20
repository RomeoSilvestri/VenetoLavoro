import os
from clickhouse_driver import Client
import numpy as np
import pandas as pd
import textdistance


# FUNZIONI---------------------------

# Connettore DB (ClickHouse)
def connect_clickhouse(query, column_names):
    client = Client(host='db-native.silv.creavista.it',
                    port=443,
                    user='luigi_frisoni',
                    password='uu1teile1Ahy',
                    database='classificazione_mbs',
                    secure=True
                    )
    result = client.execute(query)
    df = pd.DataFrame(result, columns=column_names)
    return df


# Pulitore Dataset
def clean_df(df, list_col):
    for col in list_col:
        df[col] = (df[col].apply(lambda x: x.lower()).apply(lambda x: x.replace('à', "a'").replace('á', "a'").
                                                            replace('è', "e'").replace('é', "e'").replace('ì', "i'").
                                                            replace('í', "i'").replace('ò', "o'").replace('ó', "o'").
                                                            replace('ù', "u'").replace('ú', "u'").replace('nan', '')).
                   apply(lambda x: ' '.join(x.split())))
    return df


# Funzione di Mappatura
def map_step(df_master, df_mapp, col_cod, col_nome):
    for element in df_master.index:
        cod_istat = df_master['cod_istat'][element]
        nome_com = df_master['des_comune'][element]
        # se il comune non è attivo e il codice istat non è ancora stato assegnato
        if df_master['attivo'][element] == '0' and df_master['cod_istat_NEW'][element] == 'non trovato':
            # ricerca per codice istat se presente
            if cod_istat != '':
                try:
                    new_cod = df_mapp.loc[df_mapp[col_cod[0]] == cod_istat][col_cod[1]].values[0]
                except:
                    continue
                df_master.loc[element, 'cod_istat_NEW'] = new_cod
            # ricerca per nome se codice istat non presente
            elif nome_com != '':
                try:
                    new_cod = df_mapp.loc[df_mapp[col_nome[0]] == nome_com][col_cod[1]].values[0]
                    # verifica se stato estero (solo primo file)
                    if str(new_cod) == 'nan' and df_mapp.loc[df_mapp[col_nome[0]] == nome_com][
                        col_nome[1]].values[0] == 'stato estero':
                        new_cod = 'stato estero'
                except:
                    continue
                df_master.loc[element, 'cod_istat_NEW'] = new_cod
            else:
                continue
        # se il comune è attivo lascia il codice originale
        elif df_master['attivo'][element] == '1':
            df_master.loc[element, 'cod_istat_NEW'] = cod_istat
        else:
            continue
    prov_condition = df_master['cod_istat_NEW'].str[:3].str.isnumeric()
    df_master['cod_istat_prov_NEW'] = np.where(prov_condition, df_master['cod_istat_NEW'].str[:3],
                                               df_master['cod_istat_NEW'])
    return df_master


# Calcolatore Similarità tra stringhe con distanza di Levenshtein
def levenshtein_similarity(str1, str2):
    levenshtein_distance = textdistance.levenshtein(str1, str2)
    similarity = 1 / (1 + levenshtein_distance)
    return similarity


# Calcolatore Similarità tra stringhe con distanza di Jaccard
def jaccard_similarity(str1, str2, q):
    set1 = set([str1[i:i + q] for i in range(len(str1) - q + 1)])
    set2 = set([str2[i:i + q] for i in range(len(str2) - q + 1)])
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    similarity = intersection / union
    return similarity


def comuni_similarity(df_master, df_mapp):
    df_master = df_master.copy()
    df_master['comuni_levenshtein'] = ""
    df_master['comuni_jaccard'] = ""
    df_master['cod_levenshtein'] = ""
    df_master['cod_jaccard'] = ""
    df_master['cod_prov_levenshtein'] = ""
    df_master['cod_prov_jaccard'] = ""
    df_comuni_filt = df_master[df_master['cod_istat_NEW'] == 'non trovato']

    for idx in df_comuni_filt.index:
        # print(idx)
        nome = df_comuni_filt['des_comune'][idx]
        max_levenshtein = 0
        max_jaccard = 0
        nome_levenshtein = ""
        nome_jaccard = ""
        cod_levenshtein = ""
        cod_jaccard = ""
        cod_prov_levenshtein = ""
        cod_prov_jaccard = ""

        for idy in df_mapp.index:
            nome_supporto = df_mapp['Denominazione Comune'][idy]
            cod_supporto = df_mapp['Codice del Comune associato alla variazione'][idy]
            levenshtein = levenshtein_similarity(nome, nome_supporto)
            jaccard = jaccard_similarity(nome, nome_supporto, 3)

            if levenshtein > 1 / 3 and levenshtein > max_levenshtein:
                max_levenshtein = levenshtein
                nome_levenshtein = nome_supporto
                cod_levenshtein = str(cod_supporto)
                cod_prov_levenshtein = cod_levenshtein[:3]
                if (cod_levenshtein == 'nan' and df_mapp['Denominazione Comune associato alla variazione'][idy]
                        == 'stato estero'):
                    cod_levenshtein = 'stato estero'

            if jaccard > 0.6 and jaccard > max_jaccard:
                max_jaccard = jaccard
                nome_jaccard = nome_supporto
                cod_jaccard = str(cod_supporto)
                cod_prov_jaccard = cod_jaccard[:3]
                if (cod_jaccard == 'nan' and df_mapp['Denominazione Comune associato alla variazione'][idy]
                        == 'stato estero'):
                    cod_jaccard = 'stato estero'

        df_master.loc[idx, 'comuni_levenshtein'] = nome_levenshtein
        df_master.loc[idx, 'comuni_jaccard'] = nome_jaccard
        df_master.loc[idx, 'cod_levenshtein'] = cod_levenshtein
        df_master.loc[idx, 'cod_jaccard'] = cod_jaccard
        df_master.loc[idx, 'cod_prov_levenshtein'] = cod_prov_levenshtein
        df_master.loc[idx, 'cod_prov_jaccard'] = cod_prov_jaccard

    return df_master


# FINE FUNZIONI---------------------------

# Import File Master e File ISTAT di Supporto per la Mappatura
path_input = os.getcwd() + '\\clean\\st_comuni\\input\\'
selection = ('SELECT cod_comune,cod_provincia,des_comune,cod_istat,cod_inps,cod_cap, cod_prefisso,dat_inizio,dat_fine,'
             'dtt_tmst,dat_agg,revisione,attivo from st_comuni_raw')
cl_names = ['cod_comune', 'cod_provincia', 'des_comune', 'cod_istat', 'cod_inps', 'cod_cap', 'cod_prefisso',
            'dat_inizio', 'dat_fine', 'dtt_tmst', 'dat_agg', 'revisione', 'attivo']
df_comuni = connect_clickhouse(selection, cl_names)
df_soppress = pd.read_excel(path_input + '01_Elenco comuni soppressi.xls', sheet_name='Comuni soppressi', dtype='str')
df_ridenomin = pd.read_excel(path_input + '02_Elenco_denominazioni_precedenti.xls', dtype='str')
df_variaz = pd.read_excel(path_input + '03_Variazioni_amministrative_territoriali_dal_01011991.xlsx', dtype='str')

# Pulizia Dataframe
# Cleaning File Master
df_comuni['attivo'] = df_comuni['attivo'].apply(lambda x: int(round(float(x))))
df_comuni = df_comuni.astype(str)
df_comuni.drop_duplicates(inplace=True)
df_comuni = df_comuni[(df_comuni['revisione'] == max(df_comuni['revisione'])) & (df_comuni['cod_provincia'] != '')]
df_comuni = df_comuni.reset_index(drop=True)
df_comuni['des_comune_orig'] = df_comuni['des_comune']
df_comuni.insert(3, df_comuni.columns[-1], df_comuni.pop(df_comuni.columns[-1]))
df_comuni['des_comune'] = df_comuni['des_comune'].str.lower().str.replace(' * ', '/').apply(
    lambda x: ' '.join(x.split()))
df_comuni['cod_istat_NEW'] = 'non trovato'

# Cleaning File di Supporto:
# 1) Comuni Soppressi
# 2) Comuni Ridenominati
# 3) Comuni con Variazione Amministrativa-Territoriale
df_soppress = clean_df(df_soppress, ['Denominazione Comune', 'Denominazione Comune associato alla variazione'])
df_ridenomin = clean_df(df_ridenomin,
                        ['Denominazione precedente', 'Comune cui è associata la denominazione precedente'])
df_variaz = clean_df(df_variaz, ['Denominazione Comune'])

# Mappatura
# Step1 -- Comuni Soppressi
col_cod_sop = ['Codice Comune', 'Codice del Comune associato alla variazione']
col_nome_sop = ['Denominazione Comune', 'Denominazione Comune associato alla variazione']
df_comuni = map_step(df_comuni, df_soppress, col_cod_sop, col_nome_sop)

# Step2 -- Comuni Ridenominati
col_cod_ridenomin = ['Codice di denominazione', 'Codice comune']
col_nome_ridenomin = ['Denominazione precedente', 'Comune cui è associata la denominazione precedente']
df_comuni = map_step(df_comuni, df_ridenomin, col_cod_ridenomin, col_nome_ridenomin)

# Step3 -- Comuni con Variazione Amministrativa-Territoriale
col_cod_variaz = ['Codice Comune formato alfanumerico',
                  'Codice del Comune associato alla variazione o nuovo codice Istat del Comune ']
col_nome_variaz = ['Denominazione Comune', 'Denominazione Comune associata alla variazione o nuova denominazione']
df_comuni = map_step(df_comuni, df_variaz, col_cod_variaz, col_nome_variaz)

# Step4 -- Mappatura con Algoritmo di Similarità
df_comuni = comuni_similarity(df_comuni, df_soppress)

# path_output = os.getcwd() + '\\clean\\st_comuni\\output\\'
# df_comuni.to_excel(path_output + 'df_comuni_final.xlsx', index=False)
