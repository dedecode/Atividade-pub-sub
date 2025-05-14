import smtplib
import ssl
import json
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaException, KafkaError


def disparar_email(conteudo):
    print("[DEBUG] Disparando e-mail com conteúdo:", conteudo)
    email = EmailMessage()
    email.set_content(conteudo)
    email['Subject'] = 'Alerta de Imagem'
    email['From'] = 'email remetente' #substituir
    email['To'] = 'email destinatario' #substituir

    contexto_ssl = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto_ssl) as servidor:
        servidor.login('email', 'senha') #substituir
        servidor.send_message(email)


config_kafka = {
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notifier-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

cliente = Consumer(config_kafka)
cliente.subscribe(['notifier'])

try:
    while True:
        #print("Teste...")
        mensagem = cliente.poll(timeout=1.0)

        if mensagem is None:
            continue

        if mensagem.error():
            if mensagem.error().code() == KafkaError._PARTITION_EOF:
                print(f"[INFO] Fim da partição: {mensagem.partition()}")
            else:
                raise KafkaException(mensagem.error())
        else:
            try:
                conteudo_mensagem = mensagem.value().decode('utf-8')
                print(f"[DEBUG] Mensagem recebida: {conteudo_mensagem}")

                dados = json.loads(conteudo_mensagem)
                print(f"[DEBUG] JSON decodificado: {dados}")

                texto_email = dados.get('body', 'Corpo da mensagem ausente.')
                print(f"[DEBUG] Corpo do e-mail: {texto_email}")

                disparar_email(texto_email)

            except Exception as erro:
                print(f"[ERRO] Falha ao interpretar a mensagem: {erro}")
finally:
    cliente.close()
