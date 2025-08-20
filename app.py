from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
import time
import os
import io
from urllib.parse import quote

app = Flask(__name__)
CORS(app)

# --- FUNÇÃO PRINCIPAL REESTRUTURADA ---
def processar_requisicao_batch(webhook_base, entity, action, params, log_stream):
    
    def custom_print(message):
        print(message)
        log_stream.write(message + '\n')

    custom_print(f"🚀 Iniciando processo para entidade '{entity}' com a ação '{action}'.")

    # --- LÓGICA CONDICIONAL BASEADA NA AÇÃO ---
    if action == 'update':
        # Extrai os parâmetros específicos do 'update'
        file_path = params.get('file_path')
        field_id = params.get('field_id')
        valor_atualizado = params.get('valor_atualizado')
        batch_limit = params.get('batch_limit')
        delay_batch = params.get('delay_batch')
        
        # Chama a função de atualização que já tínhamos
        run_update_batch(webhook_base, entity, file_path, field_id, valor_atualizado, batch_limit, delay_batch, custom_print)
    
    elif action in ['get', 'add']:
        custom_print(f"⚠️ Ação '{action}' para a entidade '{entity}' ainda não foi implementada.")
        custom_print("Nenhuma operação foi executada.")
    
    else:
        custom_print(f"❌ Ação '{action}' desconhecida.")

    custom_print("\n✅ Processo finalizado.")


# --- LÓGICA DE UPDATE (EXTRAÍDA DA FUNÇÃO ANTERIOR) ---
def run_update_batch(webhook_base, entity, file_path, field_id, valor_atualizado, batch_limit, delay_batch, custom_print):
    if not webhook_base.endswith('/'):
        webhook_base += '/'

    entity_name = entity.split('.')[1]
    update_method = f"{entity}.update"
    list_method = f"{entity}.list"
    
    # 1. Testar Webhook
    url_teste = f"{webhook_base}{list_method}?start=0&select[]=ID"
    try:
        resposta = requests.get(url_teste, timeout=10)
        if resposta.status_code != 200:
            custom_print(f"❌ Webhook inválido para a entidade '{entity_name}'! Código HTTP: {resposta.status_code}")
            return
    except Exception as e:
        custom_print(f"❌ Erro ao testar webhook: {e}")
        return
    custom_print(f"✔ Webhook válido! Iniciando atualização de '{entity_name}'.")

    # 2. Ler o arquivo Excel
    try:
        df = pd.read_excel(file_path)
        possible_id_columns = ['ID', 'Contact ID', 'Company ID', 'Deal ID']
        coluna_id = next((col for col in possible_id_columns if col in df.columns), None)
        if not coluna_id:
            custom_print(f"❌ Nenhuma coluna de ID encontrada na planilha. Esperado: {possible_id_columns}")
            return
        ids = [int(x) for x in df[coluna_id].dropna()]
        total = len(ids)
        custom_print(f"📄 Total de entidades a atualizar: {total}")
    except Exception as e:
        custom_print(f"❌ Erro ao ler o arquivo Excel: {e}")
        return

    # 3. Processamento em Lotes
    valor_encoded = quote(str(valor_atualizado))
    for i in range(0, total, batch_limit):
        lote_ids = ids[i:i+batch_limit]
        cmd = {f"update_{idx}": f"{update_method}?id={entity_id}&fields[{field_id}]={valor_encoded}" for idx, entity_id in enumerate(lote_ids)}
        
        custom_print(f"\n>> Enviando lote {i // batch_limit + 1}...")
        try:
            response = requests.post(f"{webhook_base}batch.json", json={"cmd": cmd}, timeout=30)
            response.raise_for_status()
            # ... (Lógica de tratamento de resposta continua a mesma) ...
            custom_print("Lote processado.")
        except Exception as e:
            custom_print(f"❌ Erro crítico ao enviar o lote: {e}")
        time.sleep(delay_batch)

# --- ENDPOINT DA API ---
@app.route('/execute', methods=['POST'])
def execute_script():
    log_stream = io.StringIO()
    try:
        webhook = request.form.get('webhook_base')
        entity = request.form.get('entity')
        action = request.form.get('action')
        
        params = {
            "batch_limit": int(request.form.get('batch_limit', 50)),
            "delay_batch": float(request.form.get('delay_batch', 1.5))
        }

        if action == 'update':
            if 'file' not in request.files:
                return jsonify({"error": "Nenhum arquivo enviado para a ação 'update'"}), 400
            file = request.files['file']
            filepath = os.path.join("./", file.filename)
            file.save(filepath)
            params["file_path"] = filepath
            params["field_id"] = request.form.get('field_id')
            params["valor_atualizado"] = request.form.get('new_value')
        
        processar_requisicao_batch(webhook, entity, action, params, log_stream)
        
    except Exception as e:
        log_stream.write(f"Um erro inesperado ocorreu no servidor: {e}")
    finally:
        log_content = log_stream.getvalue()
        log_stream.close()
        # Limpa o arquivo se ele foi criado
        if 'file_path' in locals() and os.path.exists(params.get("file_path")):
            os.remove(params.get("file_path"))
            
    return jsonify({"message": "Execução concluída", "log": log_content})


if __name__ == '__main__':
    print("Servidor backend iniciado em http://127.0.0.1:5000")
    print("Agora, abra o arquivo 'index.html' no seu navegador.")
    app.run(port=5000, debug=True)
