import gspread
import pandas as pd
from datetime import datetime
import os
import json 
import sys 
from gspread.exceptions import WorksheetNotFound, APIError # Importação crucial para checar erros

# --- Configurações ---
ID_HISTORICO = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
OUTPUT_HTML = "dashboard_historico.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_historico.html"
COLUNA_DATA = 'DATA E HORA'
COLUNA_VALOR = 'VALOR DA VENDA'

def autenticar_gspread():
    # ... (Lógica de autenticação mantida, pois está FUNCIONANDO) ...
    # Omitido por brevidade, mas use a última versão completa que eu enviei.
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
    total_vendas_global = 0
    
    try:
        gc = autenticar_gspread()
        
        # Ponto 2: Abertura da Planilha
        print("DEBUG: 2. Tentando abrir a planilha com ID: " + ID_HISTORICO)
        planilha_historico = gc.open_by_key(ID_HISTORICO)
        
        # Ponto 2.0: Confirmação da aba (worksheet)
        aba_dados = VENDAS(0)
        
        # 🚨 NOVO PONTO DE DEBUG CRÍTICO
        print(f"DEBUG: 2.0 SUCESSO. ABA ENCONTRADA: '{aba_dados.title}'. Tentando obter valores...") 

        # Ponto 2.1: Leitura dos dados (O provável ponto de falha '0')
        dados = aba_dados.get_all_values()

        # GOVERNANÇA CRÍTICA: Checa se a planilha retornou dados
        if not dados or len(dados) < 2:
             raise ValueError("Planilha Vazia ou Cabeçalho Incompleto: A planilha retornou menos de 2 linhas (cabeçalho + dados).")
        
        print(f"DEBUG: 2.2 Dados brutos lidos com SUCESSO. Total de linhas (incluindo cabeçalho): {len(dados)}")
        
        # 3. Processamento de Dados
        # ... (Resto da lógica de processamento e HTML da Versão 3.0/4.0) ...

        headers = dados[0]
        data = dados[1:]
        df = pd.DataFrame(data, columns=headers)
        
        # ... (O restante da análise e HTML - mantenha tudo da Versão 4.0) ...
        df[COLUNA_VALOR] = df[COLUNA_VALOR].astype(str).str.replace(',', '.', regex=True)
        df['Valor_Venda_Float'] = pd.to_numeric(df[COLUNA_VALOR], errors='coerce')
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        df_validos = df.dropna(subset=['Data_Datetime', 'Valor_Venda_Float']).copy()

        # ... (lógica de vendas mensais e insights) ...
        # (Omitido por brevidade, mas use o código anterior)
        
        if df_validos.empty:
             raise ValueError("Nenhum dado válido encontrado após a limpeza. Planilha contém apenas sujeira.")
        
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        vendas_mensais = df_validos.groupby('Mes_Ano')['Valor_Venda_Float'].sum().reset_index()
        # ... (resto da lógica) ...
        
        # ... (Geração do HTML) ...
        total_vendas_global = vendas_mensais['Valor_Venda_Float'].sum()
        insight_tendencia = "SUCESSO DE GOVERNANÇA!"
        table_rows = ""
        # ... (Preenchimento do HTML) ...

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard Histórico de Vendas - Tendências</title>
             <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f4f7f6; color: #333; }}
                .container {{ max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                h2 {{ color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
                .metric-box {{ padding: 15px; margin-bottom: 15px; border-radius: 6px; }}
                .insight {{ background-color: #e9ecef; border-left: 5px solid #007bff; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                th, td {{ padding: 12px; border: 1px solid #ddd; text-align: left; }}
                th {{ background-color: #007bff; color: white; }}
                .positivo {{ color: green; font-weight: bold; }}
                .negativo {{ color: red; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>📊 Análise Histórica e Tendências de Vendas (Total Global: R$ {total_vendas_global:,.2f})</h2>
                <p>Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} (Lendo {len(df_validos) if 'df_validos' in locals() else 0} registros válidos)</p>
                
                <div class="metric-box insight">
                    <h3>Insights de Tendência</h3>
                    <p>{insight_tendencia}</p>
                </div>

                <h2>📈 Vendas Consolidadas Mês a Mês</h2>
                <p>...</p>
                
            </div>
        </body>
        </html>
        """

        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Análise Histórica concluída! {OUTPUT_HTML} gerado com sucesso.")

    except (APIError, WorksheetNotFound, ValueError, Exception) as e:
        error_message = str(e) if str(e) else f"ERRO CRÍTICO SEM MENSAGEM: Falha na Leitura dos Dados (Planilha Vazia ou Aba Errada). Tipo de erro: {type(e).__name__}. ÚLTIMO DEBUG OK: 2.0"
        
        print(f"ERRO DE EXECUÇÃO FINAL: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard Histórico</h2><p>Detalhes: {error_message}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica()
