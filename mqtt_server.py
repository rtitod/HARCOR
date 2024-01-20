import paho.mqtt.client as mqtt
import json
import ssl
import subprocess

broker_address = "127.0.0.1"
port = 1883
topic = "edge"
humidity = -999
limits = 2

#requiere tener instalado mosquitto
#trabajo en proceso

# Envia mensajes a amazon
def publish_to_amazon(payload):
    amazon_broker_address = "a85vkpsrzp7kv-ats.iot.us-west-2.amazonaws.com"
    amazon_topic = "sensor/humidity"
    ca_file = "./AmazonRootCA1.pem"
    cert_file = "./e57e2e5c45c0a82b8c5b80f45a0eb67c55aba7b541cc869fb758858559c083ab-certificate.pem.crt"
    key_file = "./e57e2e5c45c0a82b8c5b80f45a0eb67c55aba7b541cc869fb758858559c083ab-private.pem.key"

    mosquitto_pub_cmd = [
        "mosquitto_pub",
        "-h", amazon_broker_address,
        "-p", "8883",
        "--cafile", ca_file,
        "--cert", cert_file,
        "--key", key_file,
        "-t", amazon_topic,
        "-m", payload,
        "-d"
    ]

    try:
        subprocess.run(mosquitto_pub_cmd, check=True)
        print("Mensaje enviado a Amazon")
    except subprocess.CalledProcessError as e:
        print(f"Error al enviar el mensaje a Amazon: {e}")

# Callback que se ejecuta cuando se recibe un mensaje en el tópico suscrito
def on_message(client, userdata, message):
    global humidity 
    payload = message.payload.decode("utf-8")
    print(f"Mensaje recibido en el tópico {message.topic}: {payload}")
    
    try:
        json_data = json.loads(payload)
        medida = json_data.get("medida")
        if humidity == -999:
            print("enviando mensaje")
            humidity = medida
            publish_to_amazon(payload)
        elif medida > humidity + limits or medida < humidity - limits:  # Ajuste en la condición
            print("enviando mensaje")
            humidity = medida
            publish_to_amazon(payload)
        else:
            print("mensaje dentro de los limites")

    except json.JSONDecodeError:
        print("Este no es un mensaje JSON válido")


client = mqtt.Client()
client.on_message = on_message

client.connect(broker_address, port, 60)
client.subscribe(topic)

try:
    print(f"Conectado al servidor MQTT. Esperando mensajes en el tópico {topic}...")
    client.loop_forever()

except KeyboardInterrupt:
    print("Desconectando...")
    client.disconnect()

