from model import verificar_validar,verificar_validar_estudo
import pika
import json

def enviar_fila_rabbitmq(status, fila):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.1.4.71',port=5672))
    channel = connection.channel()
    channel.queue_declare(queue=fila, durable=True)
    for i in verificar_validar(status):
        temp = {
            "id" : i.id,
            "estudo": i.estudo,
            "paciente" : i.paciente,
            "patientID": i.patientID,
            "file": []
        }
        for k in verificar_validar_estudo(i.id):
            temp["file"].append(k)
        channel.basic_publish(exchange='importpacs', routing_key=fila, body=json.dumps(temp))
        print(f"Enviado {fila}", temp)
    connection.close()