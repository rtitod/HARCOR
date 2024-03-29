#este script usa python3
#ejecute los siguientes comandos en consola antes de iniciar este script
#export PYTHONIOENCODING=utf8

import paho.mqtt.client as mqtt
import json
import ssl
import time

broker_address = "127.0.0.1"
port = 1883
topicsartifact = "edge/tap"
amazon_broker_address = "a85vkpsrzp7kv-ats.iot.us-west-2.amazonaws.com"
amazon_topic_artifact = "artifact/tap"
ca_file = "./AmazonRootCA1.pem"
cert_file = "./e57e2e5c45c0a82b8c5b80f45a0eb67c55aba7b541cc869fb758858559c083ab-certificate.pem.crt"
key_file = "./e57e2e5c45c0a82b8c5b80f45a0eb67c55aba7b541cc869fb758858559c083ab-private.pem.key"

# Callback que se ejecuta cuando se recibe un mensaje en el topico suscrito
def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    print("Mensaje recibido en el topico {}: {}".format(msg.topic, payload))
    client_publish = mqtt.Client()
    client_publish.connect(broker_address, port, 10)
    client_publish.loop_start()
    client_publish.publish(topicsartifact, payload, qos=1)
    print("Mensaje enviado a Edge")
    time.sleep(1)
    client_publish.disconnect()

def on_connect(client, userdata, flags, rc, properties=None):
    print("Conectado. Esperando mensajes en el topico {}".format(topicsartifact))
    client.subscribe(amazon_topic_artifact)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Conexion perdida. Codigo de resultado: {}. Intentando reconectar...".format(rc))

client_suscribe = mqtt.Client()
client_suscribe.tls_set(ca_file, certfile=cert_file, keyfile=key_file, cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
client_suscribe.on_message = on_message
client_suscribe.on_disconnect = on_disconnect
client_suscribe.on_connect = on_connect

try:
    client_suscribe.connect_async(amazon_broker_address, 8883, 60)
    print("Conectando al servidor MQTT...")
    client_suscribe.loop_forever()

except KeyboardInterrupt:
    print("Desconectando...")
    client_suscribe.disconnect()

