import gspread
import os 
import json 
from datetime import datetime

# --- CONFIGURAÇÕES DAS PLANILHAS (Definição do Ambiente) ---

# IDs das planilhas (Usando apenas o ID, não a URL completa)
PLANILHA_ORIGEM_ID = "1LuqYrfR8ry_MqCS93Mpj9_7Vu0i9RUTomJU2n69bEug"  # Vendas e Gastos (Origem)
PLANILHA_HISTORICO_ID = "1XWdRbHqY6DWOlSO-oJbBSyOsXmYhM_NEA2_yvWbfq2Y" # HISTORICO DE VENDAS E GASTOS (Destino)

# Mapeamento das Abas: {ABA_ORIGEM: ABA_DESTINO}
# Usamos minúsculas na origem e MAIÚSCULAS no destino para a distinção que você solicitou.
MAP_ABAS = {
    "vendas": "VENDAS",
    "gastos": "GASTOS"
}
# -----------------------------------------------------------


def autenticar_gspread():
    """Autentica o gspread usando a variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS."""
    credenciais_json_string = os.environ.get('GSPREAD_SERVICE_ACCOUNT_CREDENTIALS')

    if not credenciais_json_string:
        raise Exception("Variável de ambiente GSPREAD_SERVICE_ACCOUNT_CREDENTIALS não encontrada! Verifique o Secret.")

    try:
        credenciais_dict = json.loads(credenciais_json_string)
        return gspread.service_account_from_dict(credenciais_dict)
    except Exception as e:
        raise Exception(f"Erro ao carregar ou autenticar credenciais JSON: {e}")


def fazer_backup(gc, planilha_origem_id, planilha_historico_id, aba_origem_name, aba_historico_name):
    """
    Função modularizada para realizar o backup de uma aba para outra.
    """
    print(f"\n--- Iniciando Backup: {aba_origem_name.upper()} para {aba_historico_name} ---")
    
    try:
        # 1. Abre a aba de origem e pega todos os dados
        planilha_origem = gc.open_by_key(planilha_origem_id).worksheet(aba_origem_name)
        # Pega todos os valores, incluindo o cabeçalho (Linha 1)
        dados_do_mes = planilha_origem.get_all_values()
        
        # 2. Verifica se há dados novos (exclui o cabeçalho)
        dados_para_copiar = dados_do_mes[1:] 

        if not dados_para_copiar:
            print(f"Não há novos dados na aba '{aba_origem_name}' para consolidar (apenas cabeçalho).")
            return

        # 3. Abre a aba de destino (Histórico)
        planilha_historico = gc.open_by_key(planilha_historico_id).worksheet(aba_historico_name)
        
        # 4. Apêndice: Insere os dados na última linha vazia
        # value_input_option='USER_ENTERED' é crucial para manter formatos como datas e moedas.
        planilha_historico.append_rows(dados_para_copiar, value_input_option='USER_ENTERED')
        
        print(f"Backup de {len(dados_para_copiar)} linhas concluído com sucesso e consolidado na aba '{aba_historico_name}'.")

    except gspread.exceptions.WorksheetNotFound:
        print(f"ERRO: A aba '{aba_origem_name}' ou '{aba_historico_name}' não foi encontrada. Verifique o nome das abas.")
    except Exception as e:
        print(f"ERRO GRAVE durante o backup de {aba_origem_name}: {e}")
        # Uma falha aqui deve ser alarmante.
        raise


def main():
    """Função principal para orquestrar a execução do Agente."""
    hoje = datetime.now().day
    
    # -------------------------------------------------------------
    # Controle de Execução (O Agente só age no dia 1 e 16)
    # -------------------------------------------------------------
    if hoje not in [1, 16]:
        print(f"Hoje é dia {hoje}. O Agente de Backup está dormindo (aguardando o dia 1 ou 16 do mês).")
        return

    print(f"\n🚀 AGENTE DE BACKUP ATIVADO - Executando no dia {hoje}...")

    # Autentica UMA VEZ
    gc = autenticar_gspread()
    
    # Executa a função de backup para Vendas e Gastos
    for origem, destino in MAP_ABAS.items():
        fazer_backup(gc, PLANILHA_ORIGEM_ID, PLANILHA_HISTORICO_ID, origem, destino)
        
    print("\n✅ ORQUESTRAÇÃO DE BACKUP CONCLUÍDA.")


if __name__ == "__main__":
    try:
        main()
    except Exception as final_e:
        print(f"\n### FALHA CRÍTICA DO AGENTE ###\n{final_e}")
        # Em um ambiente de produção (como o GitHub Actions), o script falharia aqui.
