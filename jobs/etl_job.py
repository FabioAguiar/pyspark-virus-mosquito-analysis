"""
etl_job.py
~~~~~~~~~~
Script para execução do job Spark:
    $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        --py-files packages.zip $HOME/projects/pyspark-virus-mosquito-analysis/jobs/etl_job.py

$SPARK_HOME/bin/spark-submit --master local[*] --py-files packages.zip $HOME/projects/pyspark-virus-mosquito-analysis/jobs/etl_job.py

"""

from pyspark import SparkContext
import pyspark.pandas as pspd
from pyspark.sql.functions import year
import matplotlib.pyplot as plt


def main():
    spark = SparkContext(appName="my_etl_job")

    # Execução do pipeline ETL
    data = extract_data(spark)

    psdf = transform_data(data)

    # Análise 1: Distribuição de casos ao longo dos anos
    load_plot_1(psdf)
    # Análise 2: Identificação dos meses com maior incidência de casos
    load_plot_2(psdf)
    # Análise 3: Comparação de casos entre os diferentes vírus ao longo dos anos
    load_plot_3(psdf)
    # Analise 4: Distribuição de casos por faixa etária
    load_plot_4(psdf)        
    # Analise 5: Comparar a distribuição de casos por sexo (tp_sexo) e vírus.
    load_plot_5(psdf)
    #Análise 6: Agrupar os dados pelo nome do bairro e verificar a distribuição de casos em cada bairro 
    load_plot_6(psdf)    


    spark.stop()
    return None


def extract_data(spark):
    psdf_dengue_2019 = pspd.read_csv('data/raw/dengue_2019_recife.csv', sep=';')
    psdf_dengue_2020 = pspd.read_csv('data/raw/dengue_2020_recife.csv', sep=';')
    psdf_dengue_2021 = pspd.read_csv('data/raw/dengue_2021_recife.csv', sep=';')

    psdf_chikun_2019 = pspd.read_csv('data/raw/chikungunya_2019_recife.csv', sep=';')
    psdf_chikun_2020 = pspd.read_csv('data/raw/chikungunya_2020_recife.csv', sep=';')
    psdf_chikun_2021 = pspd.read_csv('data/raw/chikungunya_2021_recife.csv', sep=';')

    psdf_zika_2019 = pspd.read_csv('data/raw/zika_2019_recife.csv', sep=';')
    psdf_zika_2020 = pspd.read_csv('data/raw/zika_2020_recife.csv', sep=';')
    psdf_zika_2021 = pspd.read_csv('data/raw/zika_2021_recife.csv', sep=';')

    psdf_dengue_2019['virus']='DENGUE'
    psdf_dengue_2020['virus']='DENGUE'
    psdf_dengue_2021['virus']='DENGUE'
    psdf_chikun_2019['virus']='CHIKUNGUNYA'
    psdf_chikun_2020['virus']='CHIKUNGUNYA'
    psdf_chikun_2021['virus']='CHIKUNGUNYA'    
    psdf_zika_2019.rename(columns={"ano_notificacao": "notificacao_ano"}, inplace=True)
    psdf_zika_2019['virus']='ZIKA'
    psdf_zika_2020.rename(columns={"ano_notificacao": "notificacao_ano"}, inplace=True)
    psdf_zika_2020['virus']='ZIKA'
    psdf_zika_2021.rename(columns={"ano_notificacao": "notificacao_ano"}, inplace=True)
    psdf_zika_2021['virus']='ZIKA'

    psdf_dengue_2019 = psdf_dengue_2019[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia','virus']]
    psdf_dengue_2020 = psdf_dengue_2020[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_dengue_2021 = psdf_dengue_2021[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_chikun_2019 = psdf_chikun_2019[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_chikun_2020 = psdf_chikun_2020[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_chikun_2021 = psdf_chikun_2021[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_zika_2019 = psdf_zika_2019[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_zika_2020 = psdf_zika_2020[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]
    psdf_zika_2021 = psdf_zika_2021[['dt_notificacao', 'notificacao_ano', 'dt_nascimento', 'tp_sexo', 'no_bairro_residencia', 'virus']]

    psdf = pspd.concat([psdf_dengue_2019,psdf_chikun_2019,psdf_zika_2019, psdf_dengue_2020,psdf_chikun_2020,psdf_zika_2020, psdf_dengue_2021,psdf_chikun_2021,psdf_zika_2021])

    return psdf


def transform_data(df):
    psdf = df.dropna()

### Granularizando registros da data de notificação
    psdf["notificacao_mes"] = psdf["dt_notificacao"].dt.month
    psdf["notificacao_trimestre"] = psdf["dt_notificacao"].dt.quarter


    psdf['notificacao_mes_nome'] = psdf["notificacao_mes"].apply(aplicar_mes)
    psdf['notificacao_trimestre_nome'] = psdf["notificacao_trimestre"].apply(aplicar_trimestre)    

    #Removendo as colunas númericas do mês e do trimestre
    psdf = psdf[['dt_notificacao','notificacao_mes_nome','notificacao_trimestre_nome','notificacao_ano','dt_nascimento','tp_sexo','no_bairro_residencia','virus']]
    psdf.rename(columns={"notificacao_mes_nome": "notificacao_mes"}, inplace=True)
    psdf.rename(columns={"notificacao_trimestre_nome": "notificacao_trimestre"}, inplace=True)

    
    pd = psdf.to_pandas()

    pd["no_bairro_residencia_novo"] = pd["no_bairro_residencia"].map(aplicar_correcao)

    #Removendo e renomeando coluna
    pd = pd[['dt_notificacao','notificacao_mes','notificacao_trimestre','notificacao_ano','dt_nascimento','tp_sexo','no_bairro_residencia_novo','virus']]
    pd.rename(columns={"no_bairro_residencia_novo": "no_bairro_residencia"}, inplace=True)

    psdf = pspd.DataFrame(pd)

    ## Criar coluna com a idade
    psdf['ano_nasc'] = year('dt_nascimento')
    psdf['idade'] = psdf['notificacao_ano'] - psdf['ano_nasc']

    #Removendo a coluna ano_nasc
    psdf = psdf[['dt_notificacao','notificacao_mes','notificacao_trimestre','notificacao_ano','dt_nascimento', 'idade', 'tp_sexo','no_bairro_residencia','virus']]

    return psdf


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None


# Função de mapeamento para substituir os números pelos nomes dos meses
def aplicar_mes(numero):
    # Dicionário de mapeamento de números para nomes de meses
    meses_map = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro"
    }
    return meses_map[numero]

# Função de mapeamento para substituir os números pelos nomes do trimestre
def aplicar_trimestre(numero):
    # Dicionário de mapeamento de números para os trimestres
    trimestre_map = {
        1: "Q1",
        2: "Q2",
        3: "Q3",
        4: "Q4"
    }    
    return trimestre_map[numero]

# Função de mapeamento para substituir os nomes dos bairros
def aplicar_correcao(bairro):

    # Dicionário de mapeamento com a correcao dos nomes dos bairros
    correcao_bairro_residencia = {"SANTO AMARO": "SANTO AMARO",
    "BOA VIAGEM": "BOA VIAGEM",
    "IPSEP": "IPSEP",
    "JORDAO": "JORDAO",
    "IBURA": "IBURA",
    "PINA": "PINA",
    "CAMPO GRANDE": "CAMPO GRANDE",
    "SAO JOSE": "SAO JOSE",
    "CAMPINA DO BARRETO": "CAMPINA DO BARRETO",
    "ARRUDA": "ARRUDA",
    "BOMBA DO HEMETERIO": "BOMBA DO HEMETERIO",
    "ALTO JOSE BONIFACIO": "ALTO JOSE BONIFACIO",
    "VASCO DA GAMA": "VASCO DA GAMA",
    "NOVA DESCOBERTA": "NOVA DESCOBERTA",
    "PRADO": "PRADO",
    "CORDEIRO": "CORDEIRO",
    "IPUTINGA": "IPUTINGA",
    "TORROES": "TORROES",
    "VARZEA": "VARZEA",
    "AFOGADOS": "AFOGADOS",
    "BONGI": "BONGI",
    "SAN MARTIN": "SAN MARTIN",
    "JARDIM SAO PAULO": "JARDIM SAO PAULO",
    "COHAB": "COHAB",
    "COQUEIRAL": "COQUEIRAL",
    "ESTRADA DOS REMEDIOS": "ESTRADA DOS REMEDIOS",
    "JAQUEIRA": "JAQUEIRA",
    "ROSARINHO": "ROSARINHO",
    "IMBIRIBEIRA": "IMBIRIBEIRA",
    "AGUA FRIA": "AGUA FRIA",
    "FUNDAO": "FUNDAO",
    "LINHA DO TIRO": "LINHA DO TIRO",
    "GUABIRABA": "GUABIRABA",
    "CASA AMARELA": "CASA AMARELA",
    "ALTO DO MANDU": "ALTO DO MANDU",
    "MACAXEIRA": "MACAXEIRA",
    "ENGENHO DO MEIO": "ENGENHO DO MEIO",
    "MANGUEIRA": "MANGUEIRA",
    "AREIAS": "AREIAS",
    "ALTO SANTA TEREZINHA": "ALTO SANTA TEREZINHA",
    "DOIS UNIDOS": "DOIS UNIDOS",
    "PASSARINHO": "PASSARINHO",
    "ALTO JOSE DO PINHO": "ALTO JOSE DO PINHO",
    "MADALENA": "MADALENA",
    "TORRE": "TORRE",
    "ILHA DO RETIRO": "ILHA DO RETIRO",
    "BOA VISTA": "BOA VISTA",
    "TORREAO": "TORREAO",
    "CABANGA": "CABANGA",
    "COELHOS": "COELHOS",
    "TAMARINEIRA": "TAMARINEIRA",
    "SITIO DOS PINTOS": "SITIO DOS PINTOS",
    "ENCRUZILHADA": "ENCRUZILHADA",
    "BRASILIA TEIMOSA": "BRASILIA TEIMOSA",
    "CORREGO DO JENIPAPO": "CORREGO DO JENIPAPO",
    "CURADO": "CURADO",
    "GRACAS": "GRACAS",
    "CAJUEIRO": "CAJUEIRO",
    "PORTO DA MADEIRA": "PORTO DA MADEIRA",
    "MORRO DA CONCEICAO": "MORRO DA CONCEICAO",
    "DOIS IRMAOS": "DOIS IRMAOS",
    "MUSTARDINHA": "MUSTARDINHA",
    "ESTANCIA": "ESTANCIA",
    "CACOTE": "CACOTE",
    "BARRO": "BARRO",
    "SANCHO": "SANCHO",
    "BREJO DE BEBERIBE": "BREJO DE BEBERIBE",
    "CAXANGA": "CAXANGA",
    "ESPINHEIRO": "ESPINHEIRO",
    "ILHA JOANA BEZERRA": "ILHA JOANA BEZERRA",
    "MANGABEIRA": "MANGABEIRA",
    "BREJO DA GUABIRABA": "BREJO DA GUABIRABA",
    "TEJIPIO": "TEJIPIO",
    "AFLITOS": "AFLITOS",
    "CASA FORTE": "CASA FORTE",
    "ZUMBI": "ZUMBI",
    "BEBERIBE": "BEBERIBE",
    "SANTA ROSA": "SANTA ROSA",
    "SOLEDADE": "SOLEDADE",
    "TOTO": "TOTO",
    "HIPODROMO": "HIPODROMO",
    "MONTEIRO": "MONTEIRO",
    "PEIXINHOS": "PEIXINHOS",
    "JIQUIA": "JIQUIA",
    "PARNAMIRIM": "PARNAMIRIM",
    "APIPUCOS": "APIPUCOS",
    "PAISSANDU": "PAISSANDU",
    "POCO": "POCO",
    "CIDADE UNIVERSITARIA": "CIDADE UNIVERSITARIA",
    "PONTO DE PARADA": "PONTO DE PARADA",
    "RECIFE": "RECIFE",
    "DERBY": "DERBY",
    "BREJO DE GUABIRABA": "BREJO DE GUABIRABA",
    "ALTO SANTA ISABEL": "ALTO SANTA ISABEL",
    "ALUIZIO PINTO": "ALUIZIO PINTO",
    "BREJO": "BREJO",
    "CENTRO": "CENTRO",
    "OURO PRETO": "OURO PRETO",
    "RUA JERONIMO": "RUA JERONIMO",
    "SANTO ANTONIO": "SANTO ANTONIO",
    "ILHA DO LEITE": "ILHA DO LEITE",
    "SANTANA": "SANTANA",
    "PACHECO": "PACHECO",
    "GUARARAPES": "GUARARAPES",
    "JD.JORDAO": "JD.JORDAO",
    "CHAO DE ESTRELAS": "CHAO DE ESTRELAS",
    "ALTO DA BONDADE": "ALTO DA BONDADE",
    "SITIO NOVO": "SITIO NOVO",
    "CAMPO": "CAMPO GRANDE",
    "BOA": "BOA VISTA",
    "P": "PACHECO",
    "JORDAO ALTO": "JORDAO ALTO",
    "UR7 VARZEA": "UR7 VARZEA",
    "RODA DE FOGO": "RODA DE FOGO",
    "JARDIM S├O PAULO": "JARDIM SAO PAULO",
    "ENG DO MEIO": "ENGENHO DO MEIO",
    "CAMPINA": "CAMPINA DO BARRETO",
    "ChÒo de Estrela": "CHAO DE ESTRELAS",
    "CAÃOTE": "CACOTE",
    "CAþOTE": "CACOTE",
    "IPS": "IPSEP",
    "BARROI": "BARRO",
    "GRAþAS": "GRACAS",
    "ENGE DO MEIO": "ENGENHO DO MEIO",
    "Recife": "RECIFE",
    "462": "462",
    "IPUITINGA": "IPUTINGA",
    "JIGUIA": "JIGUIA",
    "PASSSARINHO": "PASSSARINHO",
    "NI": "SANTO ANTONIO",
    "BAIRRO NOVO": "BAIRRO NOVO",
    "JIQUI┴": "JIQUIA",
    "ALTO DO PASCOAL": "ALTO DO PASCOAL"
    }

    return correcao_bairro_residencia[bairro]


def criar_id_mes(mes):
    # Dicionário de mapeamento de números para nomes de meses
    meses_map = {
        "Janeiro" : 1,
        "Fevereiro" : 2,
        "Março" : 3,
        "Abril" : 4,
        "Maio" : 5,
        "Junho" : 6,
        "Julho" : 7,
        "Agosto" : 8,
        "Setembro" : 9,
        "Outubro" : 10,
        "Novembro" : 11,
        "Dezembro" : 12
    }    
    return meses_map[mes]

def correcao_sexo(caracter):
    correcao = {
    "I": "NÃO INFORMADO",
    "M": "MASCULINO",
    "F": "FEMININO"
    }
    return correcao[caracter]


# Análise 1: Distribuição de casos ao longo dos anos
def load_plot_1(psdf):
    casos_por_ano_df = pspd.DataFrame(psdf.groupby("notificacao_ano").size()).to_pandas()
    casos_por_ano_df = casos_por_ano_df.reset_index()
    casos_por_ano_df.rename(columns={0: "quantidade"}, inplace=True)

    # Plota um gráfico de barras com a distribuição de casos ao longo dos anos
    colors = ['skyblue', 'skyblue', 'skyblue']
    plt.figure(figsize=(10, 6))
    plt.bar(casos_por_ano_df['notificacao_ano'], casos_por_ano_df['quantidade'], color=colors )
    plt.xlabel('Ano da notificação')
    plt.ylabel('Quantidade')
    plt.title(' Distribuição de casos ao longo dos anos.')
    plt.xticks(casos_por_ano_df['notificacao_ano'], rotation=45) 
    plt.tight_layout()
    plt.savefig("data/plots/dist_casos_anos.png")    

    return None

# Análise 2: Identificação dos meses com maior incidência de casos
def load_plot_2(psdf):
    incidencia_casos_meses = pspd.DataFrame(psdf.groupby("notificacao_mes").size().sort_values()).to_pandas()
    incidencia_casos_meses = incidencia_casos_meses.reset_index()
    incidencia_casos_meses.rename(columns={0: "quantidade"}, inplace=True)
    incidencia_casos_meses["id_mes"] = incidencia_casos_meses["notificacao_mes"].apply(criar_id_mes)
    incidencia_casos_meses = incidencia_casos_meses.sort_values(by=['id_mes'], ascending=True)

    # Plota um gráfico de barras com a identificação dos meses com maior incidência de casos
    # colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'purple', 'orange', 'pink', 'brown', 'gray']
    colors = ['skyblue']
    plt.figure(figsize=(10, 6))
    plt.bar(incidencia_casos_meses['notificacao_mes'], incidencia_casos_meses['quantidade'], color=colors )
    plt.xlabel('Mês da notificação')
    plt.ylabel('Quantidade')
    plt.title('Identificação dos meses com maior incidência de casos.')
    plt.xticks(rotation=45) 
    plt.tight_layout()
    plt.savefig("data/plots/meses_maior_incidencia.png") 

    return None

# Análise 3: Comparação de casos entre os diferentes vírus ao longo dos anos
def load_plot_3(psdf):
    virus_ano_df = psdf.groupby(["virus", "notificacao_ano"]).size().unstack()
    virus_ano_df = virus_ano_df.reset_index()
    virus_ano_df = virus_ano_df.to_pandas()
    #virus_ano_df.set_index('virus', inplace=True)  # Define a coluna 'virus' como índice
    virus_ano_df

    # Definindo os anos para o eixo x
    anos = [2019, 2020, 2021]

    # Plotando o gráfico de linhas
    plt.figure(figsize=(10, 6))
    for index, row in virus_ano_df.iterrows():
        plt.plot(anos, row[1:], marker='o', label=row['virus'])

    # Configurando o gráfico
    plt.title('Comparação de casos entre os diferentes vírus ao longo dos anos')
    plt.xlabel('Ano')
    plt.ylabel('Número de Casos')
    plt.legend()
    plt.grid(True)
    plt.xticks(anos, rotation=45) 
    plt.tight_layout()
    plt.savefig("data/plots/virus_durante_os_anos.png") 
    return None

# Analise 4: Distribuição de casos por faixa etária
def load_plot_4(psdf):
    # Calcula a distribuição de casos por faixa etária
    group_dengue = psdf[psdf['virus'] == "DENGUE"]
    group_chikun = psdf[psdf['virus'] == "CHIKUNGUNYA"]
    group_zika = psdf[psdf['virus'] == "ZIKA"]

    group_dengue = group_dengue.sort_values(by=['idade'], ascending=False).groupby('idade').count()
    group_dengue = group_dengue['dt_notificacao']
    group_dengue.rename(columns={"dt_notificacao": "quantidade (dengue)"}, inplace=True)
    group_dengue = group_dengue.reset_index()

    group_chikun = group_chikun.sort_values(by=['idade'], ascending=False).groupby('idade').count()
    group_chikun = group_chikun['dt_notificacao']
    group_chikun.rename(columns={"dt_notificacao": "quantidade (chikun)"}, inplace=True)
    group_chikun = group_chikun.reset_index()

    group_zika = group_zika.sort_values(by=['idade'], ascending=False).groupby('idade').count()
    group_zika = group_zika['dt_notificacao']
    group_zika.rename(columns={"dt_notificacao": "quantidade (zika)"}, inplace=True)
    group_zika = group_zika.reset_index()

    # Plotando o gráfico de distribuição de casos por idade (Zika)
    plt.figure(figsize=(10, 6))
    plt.hist(group_zika['idade'], bins=20, color='skyblue', edgecolor='black')
    plt.title('Distribuição de Casos por Idade (Zika)')
    plt.xlabel('Idade')
    plt.ylabel('Número de Casos')
    plt.grid(True)
    plt.savefig("data/plots/casos_faixa_etaria_zika.png")

    # Plotando o gráfico de distribuição de casos por idade (Dengue)
    plt.figure(figsize=(10, 6))
    plt.hist(group_dengue['idade'], bins=20, color='skyblue', edgecolor='black')
    plt.title('Distribuição de Casos por Idade (Dengue)')
    plt.xlabel('Idade')
    plt.ylabel('Número de Casos')
    plt.grid(True)
    plt.savefig("data/plots/casos_faixa_etaria_dengue.png")

    # Plotando o gráfico de distribuição de casos por idade (Chikungunya)
    plt.figure(figsize=(10, 6))
    plt.hist(group_chikun['idade'], bins=20, color='skyblue', edgecolor='black')
    plt.title('Distribuição de Casos por Idade (Chikungunya)')
    plt.xlabel('Idade')
    plt.ylabel('Número de Casos')
    plt.grid(True)
    plt.savefig("data/plots/casos_faixa_etaria_chikun.png")    

    return None
# Analise 5: Comparar a distribuição de casos por sexo (tp_sexo) e vírus.
def load_plot_5(psdf):
    group_sexo_virus = psdf.groupby(['virus', 'tp_sexo']).count()
    group_sexo_virus = group_sexo_virus['dt_notificacao']
    group_sexo_virus = pspd.DataFrame(group_sexo_virus)
    group_sexo_virus.rename(columns={"dt_notificacao": "quantidade"}, inplace=True)  
    group_sexo_virus = group_sexo_virus.reset_index()
    group_sexo_virus['sexo'] = group_sexo_virus["tp_sexo"].apply(correcao_sexo)

    # Converte o resultado para um pandas DataFrame para visualização
    group_sexo_virus_df = group_sexo_virus.to_pandas()

    # Plota um gráfico de barras com a distribuição de casos por sexo
    colors = ['lightcoral', 'lightgreen','skyblue',]
    plt.figure(figsize=(10, 6))
    plt.bar(group_sexo_virus_df['sexo'], group_sexo_virus_df['quantidade'].where(group_sexo_virus_df['virus'] == 'CHIKUNGUNYA'), color=colors )
    plt.xlabel('Sexo')
    plt.ylabel('Quantidade')
    plt.title('Distribuição de casos Chikungunya.')
    plt.xticks(rotation=45) 
    plt.tight_layout()
    plt.savefig("data/plots/casos_por_sexo_chikun.png") 

    # Plota um gráfico de barras com a distribuição de casos por sexo
    plt.figure(figsize=(10, 6))
    plt.bar(group_sexo_virus_df['sexo'], group_sexo_virus_df['quantidade'].where(group_sexo_virus_df['virus'] != 'DENGUE'), color=colors)
    plt.xlabel('Sexo')
    plt.ylabel('Quantidade')
    plt.title('Distribuição de casos Dengue.')
    plt.xticks(rotation=45) 
    plt.tight_layout()
    plt.savefig("data/plots/casos_por_sexo_dengue.png")

    # Plota um gráfico de barras com a distribuição de casos por sexo
    colors = ['skyblue', 'lightcoral']
    plt.figure(figsize=(10, 6))
    plt.bar(group_sexo_virus_df['sexo'], group_sexo_virus_df['quantidade'].where(group_sexo_virus_df['virus'] == 'ZIKA'), color=colors )
    plt.xlabel('Sexo')
    plt.ylabel('Quantidade')
    plt.title('Distribuição de casos Zika.')
    plt.xticks(rotation=45) 
    plt.tight_layout() 
    plt.savefig("data/plots/casos_por_sexo_zika.png")    

    return None

## Analise 6: Agrupar os dados pelo nome do bairro e verificar a distribuição de casos em cada bairro 
def load_plot_6(psdf):
    df_incidencia_bairro = pspd.DataFrame(psdf.sort_values(by=['no_bairro_residencia'], ascending=False).groupby(['no_bairro_residencia'])['dt_notificacao'].count())
    df_incidencia_bairro = df_incidencia_bairro.reset_index()
    df_incidencia_bairro.rename(columns={"dt_notificacao": "quantidade"}, inplace=True)
    df_incidencia_bairro = df_incidencia_bairro.to_pandas()
    df_incidencia_bairro = df_incidencia_bairro.sort_values(by=['quantidade'], ascending=False)

    # Plotar o gráfico
    plt.figure(figsize=(10, 8))
    ax = df_incidencia_bairro.head(20).plot(kind='barh', x='no_bairro_residencia', y='quantidade', color='skyblue')
    plt.title('Distribuição dos 20 bairros com mais incidência')
    ax.bar_label(ax.containers[0])
    plt.xlabel('Bairro')
    plt.ylabel('Total de Casos')
    plt.tight_layout()
    plt.savefig("data/plots/vinte_bairros_com_maior_incidencia.png")    

    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
