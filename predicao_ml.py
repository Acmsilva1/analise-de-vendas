import gspread
import pandas as pd
import numpy as np
from datetime import datetime
import os
import json 
from gspread.exceptions import WorksheetNotFound, APIError 

# --- Adicionando as bibliotecas de Machine Learning ---
from sklearn.linear_model import LinearRegression 
from sklearn.metrics import mean_absolute_error

# --- CONFIGURAÇÕES DE DADOS HISTÓRICOS (VENDAS E GASTOS) ---
# VENDAS
ID_HISTORICO_VENDAS = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
ABA_VENDAS = "VENDAS"
COLUNA_VALOR_VENDA = 'VALOR DA VENDA'

# GASTOS
ID_HISTORICO_GASTOS = "1DU3oxwCLCVmmYA9oD9lrGkBx2SyI87UtPw-BDDwA9EA"
ABA_GASTOS = "GASTOS"
COLUNA_VALOR_GASTO = 'VALOR' 
COLUNA_DATA = 'DATA E HORA' 

OUTPUT_HTML = "dashboard_ml_insights.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_ml_insights.html" 
# --------------------------------------------------------------------------------

def format_brl(value):
    """Função helper para formatar valores em R$"""
    return f"R$ {value:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')

def autenticar_gspread():
    SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
    if not SHEET_CREDENTIALS_JSON:
        raise ConnectionError("Variável de ambiente 'GCP_SA_CREDENTIALS' não encontrada.")
    credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
    return gspread.service_account_from_dict(credentials_dict)

def carregar_dados_de_planilha(gc, sheet_id, aba_nome, coluna_valor, prefixo):
    print(f"DEBUG: Carregando dados: ID={sheet_id}, Aba={aba_nome}")
    try:
        planilha = gc.open_by_key(sheet_id).worksheet(aba_nome)
        dados = planilha.get_all_values()
        
        if not dados or len(dados) < 2:
             print(f"Alerta: Planilha {aba_nome} está vazia.")
             return pd.DataFrame()
             
        df = pd.DataFrame(dados[1:], columns=dados[0])
        
        # Limpeza do Valor
        df['temp_valor'] = df[coluna_valor].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=True).str.strip()
        df[f'{prefixo}_Float'] = pd.to_numeric(df['temp_valor'], errors='coerce')
        
        # Limpeza da Data
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        # Agrupamento Mensal
        df_validos = df.dropna(subset=['Data_Datetime', f'{prefixo}_Float']).copy()
        df_validos['Mes_Ano'] = df_validos['Data_Datetime'].dt.to_period('M')
        
        df_mensal = df_validos.groupby('Mes_Ano')[f'{prefixo}_Float'].sum().reset_index()
        df_mensal.columns = ['Mes_Ano', f'Total_{prefixo}']
        
        return df_mensal.set_index('Mes_Ano')
        
    except Exception as e:
        print(f"ERRO ao carregar {aba_nome}: {e}")
        return pd.DataFrame()

def carregar_e_combinar_dados(gc):
    df_vendas = carregar_dados_de_planilha(gc, ID_HISTORICO_VENDAS, ABA_VENDAS, COLUNA_VALOR_VENDA, 'Vendas')
    df_gastos = carregar_dados_de_planilha(gc, ID_HISTORICO_GASTOS, ABA_GASTOS, COLUNA_VALOR_GASTO, 'Gastos')
    
    if df_vendas.empty or df_gastos.empty:
        raise ValueError("Dados insuficientes para análise de Lucro (Vendas ou Gastos estão vazios).")

    df_combinado = pd.merge(
        df_vendas, 
        df_gastos, 
        left_index=True, 
        right_index=True, 
        how='outer' 
    ).fillna(0) 

    df_combinado['Lucro_Liquido'] = df_combinado['Total_Vendas'] - df_combinado['Total_Gastos']
    
    df_combinado = df_combinado.sort_index().reset_index()
    df_combinado['Mes_Index'] = np.arange(len(df_combinado))
    
    if len(df_combinado) < 4:
        raise ValueError(f"Dados insuficientes para ML: Apenas {len(df_combinado)} meses consolidados. Mínimo de 4 meses é recomendado.")
            
    return df_combinado

def treinar_e_prever(df_mensal):
    X = df_mensal[['Mes_Index']] 
    y = df_mensal['Lucro_Liquido'] 
    
    modelo = LinearRegression()
    modelo.fit(X, y)
    
    proximo_mes_index = df_mensal['Mes_Index'].max() + 1
    previsao_proximo_mes = modelo.predict([[proximo_mes_index]])[0]

    predicoes_historicas = modelo.predict(X)
    mae = mean_absolute_error(y, predicoes_historicas)

    ultimo_lucro_real = df_mensal['Lucro_Liquido'].iloc[-1]
    
    return previsao_proximo_mes, mae, ultimo_lucro_real

# --- NOVO: Função para gerar a tabela de auditoria ---
def gerar_tabela_auditoria(df_mensal):
    table_rows = ""
    # Itera sobre o DataFrame do mais antigo para o mais recente
    for index, row in df_mensal.iterrows():
        lucro = row['Lucro_Liquido']
        
        # Define a cor da linha com base no Lucro/Prejuízo (Governança Visual)
        lucro_class = 'lucro-positivo' if lucro >= 0 else 'lucro-negativo'
        
        table_rows += f"""
        <tr class="{lucro_class}">
            <td>{row['Mes_Ano']}</td>
            <td>{format_brl(row['Total_Vendas'])}</td>
            <td>{format_brl(row['Total_Gastos'])}</td>
            <td>{format_brl(lucro)}</td>
        </tr>
        """
    return table_rows
# ----------------------------------------------------

def montar_dashboard_ml(previsao, mae, ultimo_valor_real, df_historico):
    
    # Lógica de Classificação do Insight
    diferenca = previsao - ultimo_valor_real
    
    if previsao < 0:
        insight = f"🚨 **Previsão de PREJUÍZO!** Lucro negativo de {format_brl(abs(previsao))} esperado."
        cor = "#dc3545" # Vermelho
    elif diferenca > (ultimo_valor_real * 0.10):
        insight = f"🚀 **Crescimento de Lucro Esperado!** Aumento de {format_brl(diferenca)}."
        cor = "#28a745" # Verde
    elif diferenca < -(ultimo_valor_real * 0.10):
        insight = f"⚠️ **Risco de Queda de Lucro!** Retração de {format_brl(abs(diferenca))} esperada. Analise seus custos!"
        cor = "#ffc107" # Amarelo
    else:
        insight = f"➡️ **Estabilidade Esperada.** Lucro projetado próximo ao mês passado."
        cor = "#17a2b8" # Azul Claro

    # Geração da tabela de auditoria
    tabela_auditoria_html = gerar_tabela_auditoria(df_historico)

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Dashboard ML Insights - Previsão de Lucro Líquido</title>
         <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f4f7f6; color: #333; }}
            .container {{ max-width: 900px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h2 {{ color: #6f42c1; border-bottom: 2px solid #6f42c1; padding-bottom: 10px; }}
            .metric-box {{ padding: 20px; margin-bottom: 20px; border-radius: 8px; background-color: {cor}; color: white; text-align: center; }}
            .metric-box h3 {{ margin-top: 0; font-size: 1.5em; }}
            .metric-box p {{ font-size: 2.5em; font-weight: bold; }}
            .info-box {{ padding: 10px; border: 1px dashed #ccc; background-color: #f8f9fa; margin-top: 15px; }}
            
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 10px; border: 1px solid #ddd; text-align: left; }}
            th {{ background-color: #6f42c1; color: white; }}
            .lucro-positivo {{ background-color: #e6ffe6; }} /* Fundo claro para lucro */
            .lucro-negativo {{ background-color: #ffe6e6; }} /* Fundo claro para prejuízo */
            .text-negativo {{ color: red; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>🔮 Insights de Machine Learning: Previsão de LUCRO LÍQUIDO</h2>
            <p>Modelo: Regressão Linear Simples. Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
            
            <div class="metric-box">
                <h3>Lucro Líquido Projetado para o Próximo Mês</h3>
                <p>{format_brl(previsao)}</p>
            </div>
            
            <div class="info-box">
                <h4>Insight da Previsão:</h4>
                <p>{insight}</p>
            </div>

            <div class="info-box">
                <h4>Métricas de Qualidade (Governança de IA)</h4>
                <p>Lucro Real Mês Passado: **{format_brl(ultimo_valor_real)}**</p>
                <p>Erro Absoluto Médio Histórico (MAE): **{format_brl(mae)}**</p>
            </div>
            
            <h2>📊 Tabela de Auditoria Histórica (Base do ML)</h2>
            <p>Estes são os dados consolidados de Vendas e Gastos utilizados para treinar o modelo de previsão.</p>
            <table>
                <thead>
                    <tr>
                        <th>Mês/Ano</th>
                        <th>Vendas Totais</th>
                        <th>Gastos Totais</th>
                        <th>Lucro Líquido (Vendas - Gastos)</th>
                    </tr>
                </thead>
                <tbody>
                    {tabela_auditoria_html}
                </tbody>
            </table>

            <p style="margin-top: 20px; font-size: 0.9em; color: #777;">Dashboard hospedado em: <a href="{URL_DASHBOARD}" target="_blank">{URL_DASHBOARD}</a></p>
        </div>
    </body>
    </html>
    """
    
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"Dashboard de ML gerado com sucesso: {OUTPUT_HTML}")


# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    try:
        gc = autenticar_gspread()
        df_mensal = carregar_e_combinar_dados(gc)
        
        if not df_mensal.empty:
            previsao, mae, ultimo_lucro_real = treinar_e_prever(df_mensal)
            
            # Passando o DataFrame histórico completo para o dashboard
            montar_dashboard_ml(previsao, mae, ultimo_lucro_real, df_mensal)
        else:
            print("Execução ML interrompida por falta de dados históricos.")
            
    except Exception as e:
        error_message = str(e)
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO ML: {error_message}")
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do ML Dashboard</h2><p>Detalhes: {error_message}</p></body></html>")
