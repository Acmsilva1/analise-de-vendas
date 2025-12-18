import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import gspread # NOVO: Biblioteca para Google Sheets
import json
import sys # Para interrupção controlada

# --- REMOVIDAS FUNÇÕES ESPECÍFICAS DO GOOGLE COLAB ---
# from google.colab import files
# from IPython.display import HTML, display

# --- CONFIGURAÇÃO GOOGLE SHEETS E ARQUIVOS ---
# 🚨 IMPORTANTE: Substitua estes placeholders pelos dados da sua planilha!
SPREADSHEET_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug" 
WORKSHEET_NAME = "vendas" 

# A credencial é lida de forma segura da variável de ambiente (GitHub Secret)
SHEET_CREDENTIALS_JSON = os.environ.get('GCP_SA_CREDENTIALS') 

html_filename = 'dashboard_vendas_final.html'

# 1. Função de Tratamento de Valores Monetários (Preservada)
def parse_brl_value(value):
    """Converte strings BRL (R$ 1.234,56) para float."""
    try:
        # Lógica original: remove R$, remove pontos de milhar, troca vírgula por ponto
        cleaned_value = str(value).replace('R$', '').strip().replace('.', '').replace(',', '.')
        return float(cleaned_value)
    except:
        return None

# 2. Conexão ao Google Sheets e Leitura dos Dados
try:
    if not SHEET_CREDENTIALS_JSON:
        print("🚨 ERRO: Variável de ambiente GCP_SA_CREDENTIALS não encontrada.")
        print("Certifique-se de configurar o Secret no GitHub.")
        sys.exit(1) # Sai com erro
        
    print(f"Conectando ao Google Sheet ID: {SPREADSHEET_ID} na aba '{WORKSHEET_NAME}'...")
    
    # 2.1. Autenticação usando as credenciais do Secret do GitHub
    creds_dict = json.loads(SHEET_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    
    # 2.2. Abrir a planilha e a aba
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.worksheet(WORKSHEET_NAME)
    
    # 2.3. Obter todos os dados como lista de listas (A primeira linha é o header)
    data = worksheet.get_all_values()
    
    # 2.4. Converter para DataFrame Pandas 
    # Assume a primeira linha como cabeçalho (data[0]) e o restante como dados (data[1:])
    df = pd.DataFrame(data[1:], columns=data[0])
    
    print(f"✅ Dados lidos com sucesso! Total de {len(df)} linhas brutas.")

    # 3. Limpeza e Pré-processamento dos Dados
    # Certifique-se de que os nomes de colunas 'DATA E HORA', 'VALOR DA VENDA' e 'SABORES' 
    # no seu Google Sheet são EXATAMENTE IGUAIS aos usados aqui.
    df['Data_Venda'] = pd.to_datetime(df['DATA E HORA'], format='%d/%m/%y %H:%M', errors='coerce')
    df['Valor_Venda'] = df['VALOR DA VENDA'].apply(parse_brl_value)
    df = df.dropna(subset=['Data_Venda', 'Valor_Venda'])
    
    if df.empty:
        print("🚨 ERRO: Não há dados válidos após a limpeza. Verifique as colunas e formatos.")
        sys.exit(1)

    print(f"✅ Limpeza de dados concluída! {len(df)} registros válidos.")

except Exception as e:
    print(f"🚨 ERRO crítico ao ler o Google Sheet ou processar: {e}")
    sys.exit(1)


# --- 4. CÁLCULO DAS MÉTRICAS DE ANÁLISE (CÓDIGO ORIGINAL PRESERVADO) ---
df['Mês_Ano'] = df['Data_Venda'].dt.to_period('M').astype(str)
vendas_mensais = df.groupby('Mês_Ano')['Valor_Venda'].agg(
    Total_Vendas='sum',
    Ticket_Medio='mean',
    Num_Vendas='count'
).reset_index()

vendas_mensais['Data_Ordenacao'] = pd.to_datetime(vendas_mensais['Mês_Ano'])
vendas_mensais = vendas_mensais.sort_values(by='Data_Ordenacao').drop(columns=['Data_Ordenacao'])

# Aqui, assume-se que a coluna 'SABORES' está correta e existe
# ATENÇÃO: Se SABORES for a coluna, ela deve estar no seu Sheets
if 'SABORES' in df.columns:
    vendas_por_sabor = df.groupby('SABORES')['Valor_Venda'].sum().nlargest(5).reset_index()
    vendas_por_sabor.rename(columns={'Valor_Venda': 'Receita_Total'}, inplace=True)
else:
    print("⚠️ Aviso: Coluna 'SABORES' não encontrada. O gráfico de Top Produtos não será gerado corretamente.")
    vendas_por_sabor = pd.DataFrame({'SABORES': ['N/A'], 'Receita_Total': [0]})


# Geração dos KPIs (Dados do Resumo)
media_geral_vendas = vendas_mensais['Total_Vendas'].mean()
total_geral = vendas_mensais['Total_Vendas'].sum()
melhor_mes = vendas_mensais.loc[vendas_mensais['Total_Vendas'].idxmax()]
melhor_ticket = vendas_mensais.loc[vendas_mensais['Ticket_Medio'].idxmax()]
top_produto = vendas_por_sabor.iloc[0]

# Criação do DataFrame para a Tabela de Resumo
df_kpis = pd.DataFrame({
    'Métrica': [
        'Total Geral de Vendas',
        'Média Mensal de Faturamento',
        'Melhor Mês de Vendas',
        'Maior Ticket Médio',
        'Produto Estrela (Top 1)'
    ],
    'Valor': [
        f"R$ {total_geral:,.2f}",
        f"R$ {media_geral_vendas:,.2f}",
        f"R$ {melhor_mes['Total_Vendas']:,.2f} ({melhor_mes['Mês_Ano']})",
        f"R$ {melhor_ticket['Ticket_Medio']:,.2f} ({melhor_ticket['Mês_Ano']})",
        f"{top_produto['SABORES']} (R$ {top_produto['Receita_Total']:,.2f})"
    ]
})
print("✅ KPIs para o resumo da tabela calculados!")


# --- 5. CRIAÇÃO DO DASHBOARD PLOTLY (CÓDIGO ORIGINAL PRESERVADO) ---
fig = make_subplots(
    rows=4, cols=1,
    shared_xaxes=False,
    vertical_spacing=0.08,
    subplot_titles=(
        "📝 Resumo dos Principais Indicadores (KPIs)",
        "💸 Comparativo de Vendas Totais por Mês (vs. Média Geral)",
        "📈 Tendência do Ticket Médio Mensal (R$ por Venda)",
        "🥇 Top 5 Produtos/Sabores por Receita"
    ),
    specs=[
        [{"type": "domain"}],
        [{"type": "xy"}],
        [{"type": "xy"}],
        [{"type": "xy"}]
    ]
)

# --- Gráfico 1: Tabela de Resumo (go.Table) ---
fig.add_trace(
    go.Table(
        header=dict(values=list(df_kpis.columns), fill_color='#333', align='left', font=dict(color='white', size=14)),
        cells=dict(values=[df_kpis.Métrica, df_kpis.Valor], fill_color='#444', align='left', font=dict(color='white', size=12), height=30)
    ),
    row=1, col=1
)

# --- Gráfico 2: Vendas Totais por Mês (go.Bar) ---
fig.add_trace(go.Bar(x=vendas_mensais['Mês_Ano'], y=vendas_mensais['Total_Vendas'], name='Vendas', marker_color='#FF8C00'), row=2, col=1)

# Adiciona a linha da média geral e anotação
fig.add_trace(
    go.Scatter(
        x=vendas_mensais['Mês_Ano'],
        y=[media_geral_vendas] * len(vendas_mensais),
        mode='lines',
        name='Média Geral',
        line=dict(color='red', dash='dash'),
        hoverinfo='skip',
        showlegend=False
    ),
    row=2, col=1
)
fig.add_annotation(
    x=vendas_mensais['Mês_Ano'].iloc[-1],
    y=media_geral_vendas,
    text=f"Média Geral: R$ {media_geral_vendas:,.2f}",
    showarrow=False,
    yshift=10,
    font=dict(color="red", size=10),
    bgcolor="rgba(0,0,0,0.7)",
    borderpad=4,
    row=2, col=1
)

# --- Gráfico 3: Ticket Médio Mensal (go.Scatter) ---
fig.add_trace(go.Scatter(x=vendas_mensais['Mês_Ano'], y=vendas_mensais['Ticket_Medio'], mode='lines+markers', name='Ticket Médio', line=dict(color='#1E90FF', width=3)), row=3, col=1)

# --- Gráfico 4: Top 5 Sabores (go.Bar - Horizontal) ---
fig.add_trace(go.Bar(x=vendas_por_sabor['Receita_Total'], y=vendas_por_sabor['SABORES'], orientation='h', name='Receita', marker_color='#3CB371'), row=4, col=1)


# 6. Ajustes Finais de Layout e Exportação (CÓDIGO ORIGINAL PRESERVADO)
fig.update_layout(
    title_text=f"**DASHBOARD DE ANÁLISE DE VENDAS COMPLETA** | Fonte: Google Sheets",
    height=1500,
    template='plotly_dark',
    showlegend=False,
    hovermode="x unified"
)

# Configurações de Eixos
fig.update_xaxes(showgrid=False, zeroline=False, showticklabels=False, row=1, col=1) 
fig.update_yaxes(showgrid=False, zeroline=False, showticklabels=False, row=1, col=1) 

fig.update_yaxes(tickformat=".2f", row=2, col=1, title_text="Total Vendas (R$)")
fig.update_yaxes(tickformat=".2f", row=3, col=1, title_text="Ticket Médio (R$)")
fig.update_xaxes(title_text="Mês de Venda", row=3, col=1) 

fig.update_yaxes(title_text="Produto/Sabor", row=4, col=1)
fig.update_xaxes(title_text="Receita Total (R$)", row=4, col=1)

# Exportação do HTML
# fig.write_html() salva o novo HTML no disco do GitHub Actions
fig.write_html(html_filename, full_html=True, include_plotlyjs='cdn')

print(f"\n✨ Dashboard interativo (Final) gerado! Salvo como: {html_filename}")
print("🚀 Script finalizado. O arquivo HTML foi salvo e está pronto para ser versionado.")