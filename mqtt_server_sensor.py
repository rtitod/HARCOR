#este script usa python3
#ejecute los siguientes comandos en consola antes de iniciar este script
#export PYTHONIOENCODING=utf8

import paho.mqtt.client as mqtt
import json
import ssl
import time

broker_address = "127.0.0.1"
port = 1883
topicsensor = "edge/sensor"
humidity = -999
bounds = 0

# Envia mensajes a Amazon
def publish_to_amazon(payload):
    amazon_broker_address = "a85vkpsrzp7kv-ats.iot.us-west-2.amazonaws.com"
    amazon_topic = "sensor/humidity"
    ca_file = "./AmazonRootCA1.pem"
    cert_file = "./e57e2e5c45c0a82b8c5b80f45a0eb67c55aba7b541cc869fb758858559c083ab-certificate.pem.crt"
    key_file = "./e57e2e5c45c0a82b8c5b80f45a0eb67c55aba7b541cc869fb758858559c083ab-private.pem.key"

    client_publish = mqtt.Client()
    client_publish.tls_set(ca_file, certfile=cert_file, keyfile=key_file, cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

    try:
        client_publish.connect(amazon_broker_address, 8883, 60)
        client_publish.loop_start()
        client_publish.publish(amazon_topic, payload, qos=1)
        print("Mensaje enviado a Amazon")
    except Exception as e:
        print("Error al enviar el mensaje a Amazon: {}".format(e))
    finally:
        time.sleep(2)
        client_publish.disconnect()

# Callback que se ejecuta cuando se recibe un mensaje en el topico suscrito
def on_message(client, userdata, msg):
    global humidity 
    payload = msg.payload.decode("utf-8")
    print("Mensaje recibido en el topico {}: {}".format(msg.topic, payload))
    
    try:
        json_data = json.loads(payload)
        medida = json_data.get("medida")
        if humidity == -999:
            print("enviando mensaje")
            humidity = medida
            publish_to_amazon(payload)
        elif medida > humidity + bounds or medida < humidity - bounds:
            print("enviando mensaje")
            humidity = medida
            publish_to_amazon(payload)
        else:
            print("mensaje dentro de los limites. No se enviara")

    except ValueError:
        print("Este no es un mensaje JSON valido")


client = mqtt.Client()
client.on_message = on_message

client.connect(broker_address, port, 60)
client.subscribe(topicsensor)

try:
    print("Conectado al servidor MQTT. Esperando mensajes en el topico {}...".format(topicsensor))
    client.loop_forever()

except KeyboardInterrupt:
    print("Desconectando...")
    client.disconnect()

