import gspread
import pandas as pd
from datetime import datetime
import os
import json 
import sys 
from gspread.exceptions import WorksheetNotFound, APIError 
import numpy as np

# --- Configurações de Dados (IDs e ABAS Corrigidas para Minúsculas) ---
# VENDAS
ID_HISTORICO_VENDAS = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug" 
ABA_VENDAS = "vendas" # <--- CORRIGIDO PARA MINÚSCULAS
COLUNA_VALOR_VENDA = 'VALOR DA VENDA'

# GASTOS
ID_HISTORICO_GASTOS = "1kpyo2IpxIdllvc43WR4ijNPCKTsWHJlQDk8w9EjhwP8" 
ABA_GASTOS = "gastos" # <--- CORRIGIDO PARA MINÚSCULAS
COLUNA_VALOR_GASTO = 'VALOR' 
COLUNA_DATA = 'DATA E HORA' 

# Configurações de Saída 
OUTPUT_HTML = "dashboard_lucro_semanal.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_lucro_semanal.html"
# ---------------------------------------------------------------------


def format_brl(value):
    """Função helper para formatar valores em R$"""
    if pd.isna(value):
        return "R$ 0,00"
    return f"R$ {value:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')

def autenticar_gspread():
    print("DEBUG: 1. Iniciando autenticação...")
    try:
        SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
        if not SHEET_CREDENTIALS_JSON:
            gc = gspread.service_account(filename='credenciais.json')
            return gc
        
        credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
        gc = gspread.service_account_from_dict(credentials_dict)
        return gc

    except Exception as e:
        detailed_error = f"FALHA CRÍTICA DE AUTENTICAÇÃO: Tipo: {type(e).__name__} | Mensagem: {e}"
        raise ConnectionError(detailed_error)

# Função centralizada para carregar e limpar dados
def carregar_e_limpar_dados(gc, sheet_id, aba_nome, coluna_valor, prefixo):
    print(f"DEBUG: Carregando dados de {prefixo}: ID={sheet_id}, Aba={aba_nome}")
    try:
        planilha = gc.open_by_key(sheet_id)
        
        try:
             # Tenta acessar a aba com o nome configurado
             aba = planilha.worksheet(aba_nome)
        except WorksheetNotFound:
             # Se falhar, captura os nomes de todas as abas e informa para debug
             nomes_abas_disponiveis = [w.title for w in planilha.worksheets()]
             print(f"ERRO CRÍTICO (WorksheetNotFound): A aba configurada '{aba_nome}' NÃO FOI ENCONTRADA.")
             print(f"ABAS DISPONÍVEIS nesta planilha: {nomes_abas_disponiveis}")
             raise ValueError(f"Aba '{aba_nome}' ausente ou nome incorreto (Case-sensitive). Abas encontradas: {nomes_abas_disponiveis}")


        dados = aba.get_all_values()
        
        if not dados or len(dados) < 2:
             print(f"Alerta: Planilha {aba_nome} está vazia ou incompleta.")
             return pd.DataFrame()
             
        df = pd.DataFrame(dados[1:], columns=dados[0])
        
        if COLUNA_DATA not in df.columns or coluna_valor not in df.columns:
            raise ValueError(f"COLUNAS AUSENTES em {prefixo}: '{COLUNA_DATA}' ou '{coluna_valor}'.")

        # Limpeza do Valor
        df['temp_valor'] = df[coluna_valor].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df[f'{prefixo}_Float'] = pd.to_numeric(df['temp_valor'], errors='coerce')
        
        # Limpeza da Data
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        df_validos = df.dropna(subset=['Data_Datetime', f'{prefixo}_Float']).copy()
        
        return df_validos
        
    except Exception as e:
        # Pega erros de conexão, autenticação, ou o ValueError acima
        print(f"ERRO ao carregar {prefixo} (Planilha: {sheet_id}, Aba: {aba_nome}): {e}")
        return pd.DataFrame()


def gerar_analise_lucro_semanal():
    try:
        gc = autenticar_gspread()
        data_atual = datetime.now()
        mes_atual = data_atual.month
        ano_atual = data_atual.year
        nome_mes_vigente = data_atual.strftime('%B de %Y').capitalize()

        # 1. Carregar Dados de Vendas e Gastos (Usando IDs e Abas Corrigidas)
        df_vendas = carregar_e_limpar_dados(gc, ID_HISTORICO_VENDAS, ABA_VENDAS, COLUNA_VALOR_VENDA, 'Vendas')
        df_gastos = carregar_e_limpar_dados(gc, ID_HISTORICO_GASTOS, ABA_GASTOS, COLUNA_VALOR_GASTO, 'Gastos')
        
        if df_vendas.empty:
             raise ValueError("Dados de Vendas insuficientes para o cálculo de Lucro (Planilha vazia ou inacessível).")

        # 2. Filtrar para o Mês Vigente
        def filtrar_mes_vigente(df, prefixo):
            if df.empty:
                return pd.DataFrame()
            
            df_filtrado = df[
                (df['Data_Datetime'].dt.month == mes_atual) & 
                (df['Data_Datetime'].dt.year == ano_atual)
            ].copy()
            
            return df_filtrado

        df_vendas_mes = filtrar_mes_vigente(df_vendas, 'Vendas')
        df_gastos_mes = filtrar_mes_vigente(df_gastos, 'Gastos')
        
        # 3. Agrupamento Semanal (Foco)
        def agrupar_semanalmente(df, coluna_valor, prefixo):
            if df.empty:
                return pd.DataFrame(columns=['Semana_Ano', f'Total_{prefixo}']).set_index('Semana_Ano').to_period('W')

            df['Semana_Ano'] = df['Data_Datetime'].dt.to_period('W')
            df_semanal = df.groupby('Semana_Ano')[coluna_valor].sum().reset_index()
            df_semanal.columns = ['Semana_Ano', f'Total_{prefixo}']
            return df_semanal.set_index('Semana_Ano')

        vendas_semanais = agrupar_semanalmente(df_vendas_mes, 'Vendas_Float', 'Vendas')
        gastos_semanais = agrupar_semanalmente(df_gastos_mes, 'Gastos_Float', 'Gastos') 

        # 4. Combinação e Cálculo de Lucro Líquido
        df_combinado = pd.merge(
            vendas_semanais, 
            gastos_semanais, 
            left_index=True, 
            right_index=True, 
            how='outer' 
        ).fillna(0).sort_index().reset_index() 

        df_combinado['Lucro_Liquido'] = df_combinado['Total_Vendas'] - df_combinado['Total_Gastos']
        
        # Adicionar contagem de transações de vendas
        df_contagem = df_vendas_mes.groupby(df_vendas_mes['Data_Datetime'].dt.to_period('W'))['Vendas_Float'].size().reset_index()
        df_contagem.columns = ['Semana_Ano', 'Contagem_Vendas']
        df_contagem['Semana_Ano'] = df_contagem['Semana_Ano'].astype(str)
        
        df_combinado['Semana_Ano'] = df_combinado['Semana_Ano'].astype(str)
        df_combinado = pd.merge(df_combinado, df_contagem, on='Semana_Ano', how='left').fillna(0)
        
        if df_combinado.empty:
             raise ValueError("Combinação de dados semanal resultou em tabela vazia.")


        # 5. Análise de Tendência Semanal
        df_combinado['Lucro_Anterior'] = df_combinado['Lucro_Liquido'].shift(1)
        df_combinado['Variacao_Semanal'] = (
            (df_combinado['Lucro_Liquido'] - df_combinado['Lucro_Anterior']) / np.where(df_combinado['Lucro_Anterior'] == 0, 1, df_combinado['Lucro_Anterior']) 
        ) * 100
        
        # Métricas Totais do Mês Vigente
        total_vendas_mes = df_combinado['Total_Vendas'].sum()
        total_gastos_mes = df_combinado['Total_Gastos'].sum()
        total_lucro_mes = df_combinado['Lucro_Liquido'].sum()
        
        # Insight da Última Semana 
        if not df_combinado.empty and len(df_combinado) >= 1:
            ultima_semana = df_combinado.iloc[-1]
            tendencia = ultima_semana['Variacao_Semanal']

            if pd.isna(tendencia) or len(df_combinado) == 1:
                insight_tendencia = "Primeira semana do mês. Tendência Semana-a-Semana indisponível."
            elif ultima_semana['Lucro_Liquido'] < 0:
                insight_tendencia = f"🚨 **Prejuízo de {format_brl(abs(ultima_semana['Lucro_Liquido'])):s}!** Lucro negativo na última semana."
            elif tendencia > 15:
                insight_tendencia = f"🚀 **Forte Aumento de Lucro!** Crescimento de {tendencia:.2f}% na última semana."
            elif tendencia > 0:
                insight_tendencia = f"📈 Crescimento moderado de Lucro de {tendencia:.2f}%."
            else:
                insight_tendencia = f"📉 Queda de Lucro de {abs(tendencia):.2f}%."
        else:
            insight_tendencia = "Nenhum dado válido encontrado para análise de Lucro neste mês."


        # 6. Geração da Tabela HTML
        table_rows = ""
        for index, row in df_combinado.iterrows():
            variacao_display = f'<td class="val-col"><span class="{"positivo" if row["Variacao_Semanal"] > 0 else "negativo"}">{row["Variacao_Semanal"]:.2f}%</span></td>' if pd.notna(row["Variacao_Semanal"]) and row["Lucro_Anterior"] != 0 else '<td class="val-col">N/A</td>'
            
            lucro_class = 'lucro-positivo' if row['Lucro_Liquido'] >= 0 else 'lucro-negativo'
            lucro_display = f'<td class="{lucro_class}">{format_brl(row["Lucro_Liquido"])}</td>'
            
            table_rows += f"""
            <tr>
                <td>{row['Semana_Ano']}</td>
                <td>{format_brl(row['Total_Vendas'])}</td>
                <td>{format_brl(row['Total_Gastos'])}</td>
                {lucro_display}
                <td>{int(row['Contagem_Vendas'])}</td>
                {variacao_display}
            </tr>\n
            """

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard Semanal de Lucro Líquido</title>
             <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f0f8ff; color: #333; }}
                .container {{ max-width: 1000px; margin: auto; background: white; padding: 25px; border-radius: 10px; box-shadow: 0 6px 15px rgba(0,0,0,0.1); }}
                h2 {{ color: #008080; border-bottom: 3px solid #008080; padding-bottom: 10px; }}
                .metric-box {{ padding: 20px; margin-bottom: 20px; border-radius: 8px; background-color: #e0fafa; border-left: 6px solid #008080; font-size: 1.1em; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ padding: 12px; border: 1px solid #ddd; text-align: left; }}
                th {{ background-color: #008080; color: white; }}
                .positivo {{ color: green; font-weight: bold; }}
                .negativo {{ color: red; font-weight: bold; }}
                .lucro-positivo {{ background-color: #e6ffe6; font-weight: bold; color: #006400; }} 
                .lucro-negativo {{ background-color: #ffe6e6; font-weight: bold; color: #8b0000; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>💰 Análise Semanal de Lucro Líquido ({nome_mes_vigente})</h2>
                <h3>Total Mês Vigente: Vendas {format_brl(total_vendas_mes)} - Gastos {format_brl(total_gastos_mes)} = Lucro {format_brl(total_lucro_mes)}</h3>
                <p>Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                
                <div class="metric-box">
                    <h3>Insights Semanais de Lucro</h3>
                    <p>{insight_tendencia}</p>
                </div>

                <h2>📈 Detalhamento Semana-a-Semana</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Semana/Ano</th>
                            <th>Total Vendas</th>
                            <th>Total Gastos</th>
                            <th>Lucro Líquido</th>
                            <th>Transações (Vendas)</th>
                            <th>Tendência Semanal (WoW)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
                <p style="margin-top: 20px; font-size: 0.9em; color: #777;">Dashboard hospedado em: <a href="{URL_DASHBOARD}" target="_blank">{URL_DASHBOARD}</a></p>
            </div>
        </body>
        </html>
        """
        
        # GOVERNANÇA DE I/O: Garante o nome correto
        try:
            with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"Análise de Lucro Semanal concluída! {OUTPUT_HTML} gerado com sucesso.")

        except IOError as io_e:
            raise IOError(f"Falha na escrita do arquivo HTML no disco: {io_e}")
            
    except (APIError, WorksheetNotFound, ValueError, ConnectionError, Exception) as e:
        error_message = str(e)
        print(f"ERRO DE EXECUÇÃO FINAL: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard de Lucro Semanal</h2><p>Detalhes: {error_message}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_lucro_semanal()
