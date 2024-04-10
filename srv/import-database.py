from model import verificar_ou_inserir_estudo, verificar_ou_inserir_file
import pika
import json
import time

def ler_fila_insert():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.1.4.71', port=5672))
            channel = connection.channel()
            channel.queue_declare(queue='insert', durable=True)
            
            def callback(ch, method, properties, body):
                dcm = json.loads(body)
                std = verificar_ou_inserir_estudo(dcm.get("estudo"),
                                                  dcm.get("paciente"),
                                                  dcm.get("datahora"),
                                                  dcm.get("patientID"))
                verificar_ou_inserir_file(dcm.get("sopInsUID"),
                                          dcm.get("file"),
                                          dcm.get("file_pacs"),
                                          std)
                print("Inserindo: ", dcm)

            channel.basic_consume(queue='insert', on_message_callback=callback, auto_ack=True)
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
        except pika.exceptions.StreamLostError:
            time.sleep(1)