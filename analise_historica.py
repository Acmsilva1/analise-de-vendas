import gspread
import pandas as pd
from datetime import datetime
import os
import json 
import sys 
from gspread.exceptions import WorksheetNotFound, APIError 

# --- Configurações ---
ID_HISTORICO = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
OUTPUT_HTML = "dashboard_historico.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_historico.html"
COLUNA_DATA = 'DATA E HORA'
COLUNA_VALOR = 'VALOR DA VENDA'
NOME_ABA_DADOS = "VENDAS" 


def autenticar_gspread():
    print("DEBUG: 1. Iniciando autenticação...")
    try:
        SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
        if not SHEET_CREDENTIALS_JSON:
            gc = gspread.service_account(filename='credenciais.json')
            print("DEBUG: 1.2 Autenticação via arquivo local concluída com SUCESSO (Apenas para testes locais).")
            return gc
        
        print(f"DEBUG: 1.1 SUCESSO: Secret encontrado. Tentando json.loads... (Tamanho: {len(SHEET_CREDENTIALS_JSON)})")
        credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
        gc = gspread.service_account_from_dict(credentials_dict)
        print("DEBUG: 1.2 Autenticação via Secret concluída com SUCESSO.")
        return gc
    except Exception as e:
        detailed_error = f"FALHA CRÍTICA DE AUTENTICAÇÃO: Tipo: {type(e).__name__} | Mensagem: {e}"
        print(f"ERRO CRÍTICO DE AUTENTICAÇÃO DETALHADO: {detailed_error}")
        raise ConnectionError(detailed_error)


def gerar_analise_historica():
    try:
        gc = autenticar_gspread()
        planilha_historico = gc.open_by_key(ID_HISTORICO)
        
        try:
             aba_dados = planilha_historico.worksheet(NOME_ABA_DADOS)
        except WorksheetNotFound:
             raise WorksheetNotFound(f"A aba '{NOME_ABA_DADOS}' não foi encontrada. Verifique o nome.")
        
        print(f"DEBUG: 2.0 SUCESSO. ABA ENCONTRADA: '{aba_dados.title}'. Tentando obter valores...") 

        dados = aba_dados.get_all_values()

        if not dados or len(dados) < 2:
             raise ValueError("Planilha Vazia ou Cabeçalho Incompleto: Retornou menos de 2 linhas.")
        
        # 3. Processamento de Dados
        headers = dados[0]
        data = dados[1:]
        df = pd.DataFrame(data, columns=headers)
        
        # 🚨 DEBUG CRÍTICO 3.1: Confirmação de cabeçalhos
        print(f"DEBUG: 3.1 Cabeçalhos lidos (ATENÇÃO AQUI): {headers}")

        # 🚨 GOVERNANÇA: Checa se as colunas chave existem (Case Sensitive)
        if COLUNA_DATA not in df.columns or COLUNA_VALOR not in df.columns:
            missing_cols = [c for c in [COLUNA_DATA, COLUNA_VALOR] if c not in df.columns]
            raise ValueError(f"COLUNAS AUSENTES: A planilha não contém as colunas chave: {missing_cols}. Verifique o uso de maiúsculas/minúsculas, acentos e espaços.")

        # 🚨 DEBUG CRÍTICO 3.2: Exibição das primeiras linhas (com dados brutos)
        print(f"DEBUG: 3.2 Primeiras 5 linhas brutas (Atenção ao FORMATO):\n{df[[COLUNA_DATA, COLUNA_VALOR]].head(5).to_string()}")


        # 4. Tratamento e Limpeza
        
        # 4.1 Limpeza do Valor (Remove R$ e substitui vírgula por ponto para o Pandas)
        df['temp_valor'] = df[COLUNA_VALOR].astype(str).str.replace('R$', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df['Valor_Venda_Float'] = pd.to_numeric(df['temp_valor'], errors='coerce')
        
        # 4.2 Limpeza da Data (dayfirst=True para BR)
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        # 🚨 DEBUG CRÍTICO 4.3: Quantos NaNs foram gerados?
        n_nan_valor = df['Valor_Venda_Float'].isna().sum()
        n_nan_data = df['Data_Datetime'].isna().sum()
        print(f"DEBUG: 4.3 Linhas convertidas para NaN (Valor): {n_nan_valor}. (Data): {n_nan_data}. Total Bruto: {len(df)}.")
        
        # 4.4 Filtragem
        df_validos = df.dropna(subset=['Data_Datetime', 'Valor_Venda_Float']).copy()
        
        # 4.5 Checagem Final (o erro que você viu)
        if df_validos.empty:
             # Este erro só é levantado se 100% dos dados brutos foram rejeitados.
             raise ValueError("Nenhum dado válido encontrado após a limpeza. Planilha contém apenas sujeira ou colunas incorretas.")
        
        print(f"DEBUG: 4.6 {len(df_validos)} linhas válidas prontas para análise.")

        # ... (O restante da lógica de análise e HTML, que está correta) ...

        # 5. Análise e Geração do HTML (Omitido por brevidade, mas use o código completo da última versão)
        # ...

        # Recalcule as variáveis para o HTML (exemplo)
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        vendas_mensais = df_validos.groupby('Mes_Ano')['Valor_Venda_Float'].sum().reset_index()
        vendas_mensais['Mes_Ano'] = vendas_mensais['Mes_Ano'].astype(str) 
        # ... (Cálculo de variação) ...
        total_vendas_global = vendas_mensais['Valor_Venda_Float'].sum()
        insight_tendencia = "SUCESSO DE GOVERNANÇA!"
        table_rows = ""
        # ... (Geração de HTML) ...

        html_content = f""""""
        
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Análise Histórica concluída! {OUTPUT_HTML} gerado com sucesso.")

    except (APIError, WorksheetNotFound, ValueError, Exception) as e:
        error_message = str(e) if str(e) else f"ERRO INDEFINIDO. Revise o LOG para ver o último 'DEBUG:'. Tipo de erro: {type(e).__name__}."
        
        print(f"ERRO DE EXECUÇÃO FINAL: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard Histórico</h2><p>Detalhes: {error_message}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica()
