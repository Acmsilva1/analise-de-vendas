import gspread
import os 
import json 
from datetime import datetime
import sys

# --- CONFIGURAÇÕES DAS PLANILHAS (Definição do Ambiente) ---

# IDs das planilhas (APENAS o ID, sem a URL completa)
PLANILHA_ORIGEM_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug"  # Vendas e Gastos (Origem)
PLANILHA_HISTORICO_ID = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y" # HISTORICO DE VENDAS E GASTOS (Destino)

# Mapeamento das Abas: {ABA_ORIGEM: ABA_DESTINO}
# Origem (minúscula) -> Destino (MAIÚSCULA), conforme sua regra.
MAP_ABAS = {
    "vendas": "VENDAS",
    "gastos": "GASTOS"
}
# -----------------------------------------------------------


def autenticar_gspread():
    """
    Autentica o gspread usando a variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS.
    Este método é crucial para a segurança (governança de credenciais).
    """
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')

    if not credenciais_json_string:
        # Se não encontrar a credencial, é uma falha de segurança/configuração.
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada! Verifique o Secret no GitHub.")

    try:
        # Carrega o JSON das credenciais e autentica.
        credenciais_dict = json.loads(credenciais_json_string)
        return gspread.service_account_from_dict(credenciais_dict)
    except Exception as e:
        raise Exception(f"Erro ao carregar ou autenticar credenciais JSON: {e}")


def fazer_backup(gc, planilha_origem_id, planilha_historico_id, aba_origem_name, aba_historico_name):
    """
    Função modularizada para realizar o backup de uma aba para a aba histórica.
    """
    print(f"\n--- Iniciando Backup: {aba_origem_name.upper()} para {aba_historico_name} ---")
    
    try:
        # 1. Abre a aba de origem e pega todos os dados
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        # Pega todos os valores (inclui o cabeçalho)
        dados_do_mes = planilha_origem.get_all_values()
        
        # 2. Verifica se há dados novos (dados_do_mes[1:] exclui o cabeçalho)
        dados_para_copiar = dados_do_mes[1:] 

        if not dados_para_copiar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para consolidar (apenas cabeçalho).")
            return

        # 3. Abre a aba de destino (Histórico)
        planilha_historico = gc.open_by_key(planilha_historico_id).worksheet(aba_historico_name)
        
        # 4. Apêndice: Insere os dados na última linha vazia.
        # USER_ENTERED é vital para preservar formatos como datas e moedas.
        planilha_historico.append_rows(dados_para_copiar, value_input_option='USER_ENTERED')
        
        print(f"Backup de {len(dados_para_copiar)} linhas concluído com sucesso e consolidado na aba '{aba_historico_name}'.")

    except gspread.exceptions.WorksheetNotFound as e:
        print(f"ERRO: A aba '{aba_origem_name}' ou '{aba_historico_name}' não foi encontrada.")
        # Levantar exceção para que o GitHub Actions marque a execução como falha.
        raise RuntimeError(f"Falha na validação da Planilha: {e}") 
    except Exception as e:
        print(f"ERRO GRAVE durante o backup de {aba_origem_name}: {e}")
        raise


def main():
    """Função principal para orquestrar a execução e controlar a governança de tempo."""
    
    # Verifica se a variável de ambiente de forçar execução manual está presente.
    # Ela será 'true' apenas em acionamentos manuais via GitHub Actions.
    # Usamos .lower() pois inputs de GH Actions podem vir como string 'True'.
    FORCA_EXECUCAO = os.environ.get('FORCA_EXECUCAO_MANUAL', 'false').lower() == 'true'
    
    hoje = datetime.now().day
    
    # -------------------------------------------------------------
    # Controle de Execução (O Agente só executa se for dia 1/16 OU se for forçado)
    # -------------------------------------------------------------
    
    if hoje not in [1, 16] and not FORCA_EXECUCAO:
        # Se não é dia de backup E não foi forçado, encerra elegantemente.
        print(f"Hoje é dia {hoje}. O Agente de Backup está dormindo (aguardando o dia 1 ou 16 do mês).")
        # sys.exit(0) é usado para encerrar o script sem erro (exit code 0).
        sys.exit(0) 

    # Mensagem de Log
    if FORCA_EXECUCAO:
         print("\n🚨 AGENTE DE BACKUP ATIVADO (MANUAL OVERRIDE) - Executando sob demanda...")
    else:
         print(f"\n🚀 AGENTE DE BACKUP ATIVADO - Executando no dia {hoje}...")
    
    # 1. Autentica UMA VEZ
    gc = autenticar_gspread()
    
    # 2. Executa a função de backup para Vendas e Gastos (duas passagens)
    for origem, destino in MAP_ABAS.items():
        fazer_backup(gc, PLANILHA_ORIGEM_ID, PLANILHA_HISTORICO_ID, origem, destino)
        
    print("\n✅ ORQUESTRAÇÃO DE BACKUP CONCLUÍDA.")


if __name__ == "__main__":
    try:
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\nFalha ao executar a rotina. Verifique as credenciais ou os IDs/Nomes das abas.")
        # Retorna um código de erro para o GitHub Actions
        sys.exit(1)
