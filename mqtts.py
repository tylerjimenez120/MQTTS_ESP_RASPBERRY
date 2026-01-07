import paho.mqtt.client as mqtt
import csv
import json
import os
from datetime import datetime

# --- CONFIGURACIÓN ---
MQTT_HOST = "192.168.100.52"
MQTT_PORT = 8883
MQTT_TOPIC = "lab/ens160_aht21/data"
CA_CERT = "/etc/mosquitto/certs/ca.crt" # Cambia esto a la ruta real de tu certificado
CSV_FILE = "datos_sensores.csv"

# --- FUNCIONES ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conectado al broker. Suscribiéndose a: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Error de conexión. Código: {rc}")

def on_message(client, userdata, msg):
    try:
        # Decodificar el JSON del ESP32
        payload = json.loads(msg.payload.decode())

        # Preparar la línea de datos con timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data_row = [
            timestamp,
            payload.get("temp"),
            payload.get("hum"),
            payload.get("tvoc"),
            payload.get("eco2"),
            payload.get("iaq"),
            payload.get("status")
        ]

        # Escribir en el CSV (modo append 'a')
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            # Si el archivo es nuevo, escribir cabeceras
            if not file_exists:
                writer.writerow(["Timestamp", "Temp", "Hum", "TVOC", "eCO2", "IAQ", "Status"])
            writer.writerow(data_row)

        print(f"[{timestamp}] Datos guardados: {payload}")

    except Exception as e:
        print(f"Error procesando mensaje: {e}")

# --- INICIO DEL CLIENTE ---
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Configurar TLS (Asegúrate de que coincida con tu configuración de Mosquitto)
client.tls_set(ca_certs=CA_CERT)
client.tls_insecure_set(True) # Útil si el CN del cert no coincide con la IP

client.connect(MQTT_HOST, MQTT_PORT, 60)

print("Iniciando grabador local...")
client.loop_forever()
