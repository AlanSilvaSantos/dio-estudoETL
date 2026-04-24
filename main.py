import pandas as pd
import json
from pathlib import Path
import requests

# sdw2026_api_url = 'https://sdw-2023-prd.up.railway.app'
api_url = 'http://localhost:9000/'
PATH_JSON = r'C:\Python\DIO\ETL_IA_Generativa\arquivo_json'
PATH_EXCEL = r'C:\Python\DIO\ETL_IA_Generativa\arquivo_excel'

##################TOKEN############################## 
def autenticar_omni(login: str, senha: str) -> bool:
	parametros_url = {
		"login": login,
		"senha": senha
		}
	
	url = f'{api_url}/api/usuario/autenticar'
	response = requests.get(url, params=parametros_url)
	#print(f"URL chamada: {response.url}")
	if response.status_code == 200:
		dados_resposta = response.json()
		token = dados_resposta.get('data', {}).get('tokenAcesso')
		return token # Retorna o token para uso futuro
	
	# Se o status_code não for 200 (ex: 401 Não Autorizado), retorna vazio
	return None

############################NCM#######################
def buscar_ncms(token: str) -> list:
    # Substitua pela rota exata da sua API que lista os NCMs
    url = f'{api_url}/api/ncm' 
    
    # 1. Montamos o dicionário de Cabeçalhos (Headers)
    # Note o espaço obrigatório entre a palavra 'Bearer' e o token
    cabecalhos = {
        "Authorization": f"Bearer {token}"
    }
    
    # 2. Fazemos a requisição passando o argumento 'headers'
    response = requests.get(url, headers=cabecalhos)
    
    # 3. Tratamento de Sucesso
    if response.status_code == 200:
        dados_resposta = response.json()
        
        # Seguindo o padrão da sua API que vimos no login, 
        # é provável que os dados venham dentro da chave 'data'.
        # O segundo argumento do .get() é uma lista vazia [] para caso não encontre 'data'
        lista_ncms = dados_resposta.get('data', [])
        return lista_ncms
        
    # 4. Tratamento de Erro (Ex: 401 Unauthorized se o token expirou)
    else:
        print(f"Erro na requisição. Código: {response.status_code}")
        print(f"Detalhe: {response.text}") # Imprime o motivo do erro devolvido pela API
        return []

def ler_arquivos_locais(diretorio: str, formato: str) -> pd.DataFrame:
    path = Path(diretorio)
    lista_dfs = []
    
    extensao = '*.json' if formato == 'json' else '*.xlsx'
    arquivos = list(path.glob(extensao))
    
    for arquivo in arquivos:
        if formato == 'json':
            # 1. Lemos o arquivo JSON bruto primeiro
            with open(arquivo, 'r', encoding='utf-8') as f:
                dados_brutos = json.load(f)
            
            # 2. Extraímos apenas a lista que está em 'Nomenclaturas'
            # Convertemos essa lista específica em um DataFrame
            df_temp = pd.DataFrame(dados_brutos.get('Nomenclaturas', []))
            
        else:
            df_temp = pd.read_excel(arquivo)
        
        lista_dfs.append(df_temp)

    if not lista_dfs:
        return pd.DataFrame()

    df_final = pd.concat(lista_dfs, ignore_index=True)
    
    # 3. Limpeza do campo 'Codigo' (remover os pontos)
    # Verificamos se a coluna existe antes de tentar limpar
    coluna_alvo = 'Codigo' if 'Codigo' in df_final.columns else 'codigoNcm'
    
    if coluna_alvo in df_final.columns:
        # Transformamos em string e removemos o ponto
        df_final[coluna_alvo] = df_final[coluna_alvo].astype(str).str.replace('.', '', regex=False)
    
    return df_final





def validar_e_salvar_resultado(df_local: pd.DataFrame, lista_ncms_validos: list, nome_saida: str):
    if df_local.empty:
        print("Nenhum dado encontrado para validar.")
        return

    # Definimos qual coluna vamos validar (ajustando o nome conforme o JSON)
    col_codigo = 'Codigo' if 'Codigo' in df_local.columns else 'codigoNcm'

    # Validação: Criando a coluna de Status
    df_local['Status_NCM'] = df_local[col_codigo].apply(
        lambda x: 'Vigente' if str(x) in lista_ncms_validos else 'Expirado'
    )

    # Organizando as colunas para o Excel final
    df_resultado = df_local[[col_codigo, 'Status_NCM']].copy()
    df_resultado.columns = ['NCM', 'Status'] # Renomeando para ficar bonito no Excel

    df_resultado.to_excel(nome_saida, index=False)
    print(f"Relatório gerado: {nome_saida}")



# --- Execução do Fluxo ---
# --- Juntando as peças (Fluxo Completo) ---
result_autenticacao = autenticar_omni('totvs', 'totvs')
# 2. Se o token for válido, buscamos os NCMs
if result_autenticacao:
    print("Autenticado com sucesso! Buscando NCMs...\n")
    
    ncms = buscar_ncms(result_autenticacao)
    lista_codigos_ncm = [item.get('codigoNcm') for item in ncms] #List Comprehension
    
    print(f"Total de NCMs retornados: {len(ncms)}")
    
    # Se a lista não estiver vazia, mostramos o primeiro NCM para entender a estrutura
    if ncms:
        #print("\nEstrutura do primeiro NCM retornado:")
        #print(ncms[0])
        #print("\n")
        print(lista_codigos_ncm[:])
else:
    print("Não foi possível obter o token para prosseguir.")

# 1. Pegamos a lista de NCMs oficiais da API (usando a lógica da aula anterior)
# Aqui assumimos que 'lista_codigos_ncm' é aquela lista de strings que extraímos
ncms_da_api = lista_codigos_ncm 

# 2. Lemos os arquivos (Exemplo configurado para JSON)
# Você pode trocar para PATH_EXCEL e 'excel' conforme sua necessidade
df_origem = ler_arquivos_locais(PATH_JSON, 'json')
print(df_origem)

# 3. Validamos e Gravamos
validar_e_salvar_resultado(df_origem, ncms_da_api, 'Resultado_Validacao_NCM.xlsx')




#3º Consultar validade dos NCM no portal "https://portalunico.siscomex.gov.br/classif/#/nomenclatura/tabela?perfil=publico"



#4º Validar se o NCM ainda está ativo
