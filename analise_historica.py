import gspread
import pandas as pd
from datetime import datetime
import os
import json 

# --- Configurações ---
ID_HISTORICO = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y"
OUTPUT_HTML = "dashboard_historico.html"
URL_DASHBOARD = "https://acmsilva1.github.io/analise-de-vendas/dashboard_historico.html"
COLUNA_DATA = 'DATA E HORA'
COLUNA_VALOR = 'VALOR DA VENDA'

def autenticar_gspread():
    """
    Função de autenticação robusta, buscando credenciais do secret do GitHub Actions
    ou do arquivo local, garantindo código limpo e portabilidade.
    """
    print("DEBUG: Iniciando autenticação...")
    try:
        # Tenta carregar o JSON de credenciais da variável de ambiente (CI/CD)
        SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS')
        
        if SHEET_CREDENTIALS_JSON:
            print("DEBUG: Variável GCP_SA_CREDENTIALS encontrada. (Tamanho: {})".format(len(SHEET_CREDENTIALS_JSON)))
            credentials_dict = json.loads(SHEET_CREDENTIALS_JSON) 
            gc = gspread.service_account_from_dict(credentials_dict)
            print("DEBUG: Autenticação via Secret concluída com SUCESSO.")
            return gc
        else:
            print("DEBUG: GCP_SA_CREDENTIALS não encontrada. Tentando credenciais.json.")
            gc = gspread.service_account(filename='credenciais.json')
            print("DEBUG: Autenticação via arquivo local concluída com SUCESSO.")
            return gc

    except Exception as e:
        # Lançamos uma exceção clara para evitar o erro vazio
        # Capturamos o erro e adicionamos contexto
        context_error = f"Falha na autenticação do Google Sheets. O Secret pode estar mal formatado ou o JSON vazio. Erro: {e}"
        print(f"ERRO DE AUTENTICAÇÃO: {context_error}")
        raise ConnectionError(context_error)


def gerar_analise_historica():
    total_vendas_global = 0
    
    try:
        # 1. Autenticação
        gc = autenticar_gspread()
        
        print("DEBUG: Tentando abrir a planilha...")
        # 2. Leitura da Planilha
        planilha_historico = gc.open_by_key(ID_HISTORICO).worksheet(0)
        
        dados = planilha_historico.get_all_values()
        headers = dados[0]
        data = dados[1:]

        df = pd.DataFrame(data, columns=headers)
        print(f"DEBUG: Planilha lida. {len(df)} linhas brutas.")
        
        # 3. Tratamento e Limpeza (Resto da lógica mantida)
        
        df[COLUNA_VALOR] = df[COLUNA_VALOR].astype(str).str.replace(',', '.', regex=True)
        df['Valor_Venda_Float'] = pd.to_numeric(df[COLUNA_VALOR], errors='coerce')
        
        # CORREÇÃO DE GOVERNANÇA DE DADOS: dayfirst=True para formato DD/MM/YYYY (BR)
        df['Data_Datetime'] = pd.to_datetime(df[COLUNA_DATA], errors='coerce', dayfirst=True)
        
        df.dropna(subset=['Data_Datetime', 'Valor_Venda_Float'], inplace=True)
        
        # ... (Análise de Tendências e Geração do HTML) ...
        # (O código é grande, assumo que você irá substituir a lógica restante com o que eu te dei anteriormente, que estava correto)

        # 4. Geração da Tabela (Usando a lógica de string para evitar o bug de formatação)
        df_validos = df.copy() # Use a lógica de análise para gerar 'vendas_mensais' e 'total_vendas_global'
        # Se você usar o código anterior (o que gerou a tabela por iteração), substitua o bloco aqui.
        
        # Exemplo de Geração de Tabela (Retorno para a forma mais simples e robusta)
        
        df['Mes_Ano'] = df['Data_Datetime'].dt.to_period('M')
        vendas_mensais = df.groupby('Mes_Ano')['Valor_Venda_Float'].sum().reset_index()
        vendas_mensais['Mes_Ano'] = vendas_mensais['Mes_Ano'].astype(str) 

        vendas_mensais['Vendas_Anteriores'] = vendas_mensais['Valor_Venda_Float'].shift(1)
        vendas_mensais['Variacao_Mensal'] = (
            (vendas_mensais['Valor_Venda_Float'] - vendas_mensais['Vendas_Anteriores']) / vendas_mensais['Vendas_Anteriores']
        ) * 100
        total_vendas_global = vendas_mensais['Valor_Venda_Float'].sum()
        
        if not vendas_mensais.empty:
            ultimo_mes = vendas_mensais.iloc[-1]
            tendencia = ultimo_mes['Variacao_Mensal']
            # Simplificação dos insights para o código
            insight_tendencia = f"Tendência calculada. Último mês: {tendencia:.2f}%" if pd.notna(tendencia) else "Sem tendência."
        else:
             insight_tendencia = "Nenhum dado válido encontrado após a limpeza."

        table_rows = ""
        for index, row in vendas_mensais[['Mes_Ano', 'Valor_Venda_Float', 'Variacao_Mensal']].iterrows():
             # Formata a variação com classe CSS
            variacao_display = f'<td class="val-col"><span class="{"positivo" if row["Variacao_Mensal"] > 0 else "negativo"}">{row["Variacao_Mensal"]:.2f}%</span></td>' if pd.notna(row["Variacao_Mensal"]) else '<td class="val-col">N/A</td>'
            
            # Formata o valor de venda (mantendo a formatação BRL)
            venda_display = f'<td class="val-col">R$ {row["Valor_Venda_Float"]:,.2f}</td>'
            
            table_rows += f"<tr><td>{row['Mes_Ano']}</td>{venda_display}{variacao_display}</tr>\n"


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
                <p>Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} (Lendo {len(df_validos)} registros válidos)</p>
                
                <div class="metric-box insight">
                    <h3>Insights de Tendência</h3>
                    <p>{insight_tendencia}</p>
                </div>

                <h2>📈 Vendas Consolidadas Mês a Mês</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Mês/Ano</th>
                            <th>Total de Vendas</th>
                            <th>Tendência Mensal</th>
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

        # 5. Salva o HTML
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Análise Histórica concluída! {OUTPUT_HTML} gerado com sucesso.")

    except Exception as e:
        # 🚨 DEFESA CONTRA ERROS VAZIOS: Se a mensagem 'e' for vazia, adiciona uma mensagem útil.
        error_message = str(e) if str(e) else "ERRO CRÍTICO SEM MENSAGEM: Falha na autenticação (Secret JSON) ou na leitura da Planilha. Revise o LOG do GitHub Actions para a linha 'DEBUG'."
        
        print(f"ERRO DE EXECUÇÃO: {error_message}")
        # Gerando HTML de erro que mostra a exceção completa
        with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
             f.write(f"<html><body><h2>Erro Crítico na Geração do Dashboard Histórico</h2><p>Detalhes: {error_message}</p></body></html>")
        
if __name__ == "__main__":
    gerar_analise_historica()
