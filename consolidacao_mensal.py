import gspread
import os # Importar para acessar variáveis de ambiente
import json # Importar para tratar a string JSON

# 1. Autenticação (Agora, a autenticação busca a credencial da variável de ambiente segura)
# --------------------------------------------------------------------------------------
# ⚠️ Aqui está a mudança crucial!
# 1. Pega a string JSON do Secret do GitHub (variável de ambiente).
credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')

if not credenciais_json_string:
    raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada! Configure-a como Secret no GitHub.")

# 2. Converte a string JSON em um objeto Python (dicionário).
credenciais_dict = json.loads(credenciais_json_string)

# 3. Autentica usando o dicionário (em vez do caminho do arquivo).
gc = gspread.service_account_from_dict(credenciais_dict)
# --------------------------------------------------------------------------------------

# 2. ABRIR AS PLANILHAS
planilha_origem = gc.open_by_key("1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug").worksheet(0)
dados_do_mes = planilha_origem.get_all_values()

# Abre a planilha de destino (o Histórico Anual no Google Drive)
planilha_historico = gc.open_by_key("1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y").worksheet(0)

# 3. APÊNDICE: Insere os dados APÓS a última linha preenchida
planilha_historico.append_rows(dados_do_mes[1:], value_input_option='USER_ENTERED')

print("Backup mensal concluído e consolidado no Histórico Anual.")

# AGORA É COM VOCÊ: Após a confirmação do backup, você pode apagar
# as linhas da planilha RAW do Forms manualmente.
