from flask import Flask, request, jsonify
import json
import os
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import datetime

app = Flask(__name__)

TAXIS_FILE = 'taxis.json'

# Helper function to load taxis from the file
def load_taxis():
    if os.path.exists(TAXIS_FILE):
        with open(TAXIS_FILE, 'r') as f:
            return json.load(f)
    return []

# Helper function to save taxis to the file
def save_taxis(taxis):
    with open(TAXIS_FILE, 'w') as f:
        json.dump(taxis, f, indent=4)

def generate_certificates():
    """
    Genera un certificado autofirmado y una clave privada si no existen.
    """
    if not os.path.exists("cert.pem") or not os.path.exists("key.pem"):
        # Generar clave privada
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        # Crear certificado autofirmado
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, u"San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"My Company"),
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
        ])
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=365)
        ).add_extension(
            x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
            critical=False,
        ).sign(key, hashes.SHA256())

        # Guardar clave privada
        with open("key.pem", "wb") as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ))

        # Guardar certificado
        with open("cert.pem", "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

@app.route('/register_taxi', methods=['PUT'])
def register_taxi():
    """Endpoint to register a taxi."""
    data = request.get_json()
    if not data or 'id' not in data:
        return jsonify({'error': 'Taxi ID is required'}), 400

    taxis = load_taxis()
    taxi_id = data['id']

    # Check if taxi is already registered
    if any(taxi['id'] == taxi_id for taxi in taxis):
        return jsonify({'message': f'Taxi {taxi_id} is already registered'}), 200

    # Add the new taxi
    new_taxi = {
        "id": taxi_id,
        "coordenada_origen": {"x": 1, "y": 1},
        "coordenada_destino": {"x": None, "y": None, "id": ""},
        "estado": "verde",
        "disponible": True,
        "verificado": False,
        "recogido": False,
        "returning_to_base": False,
        "cliente": {"x": None, "y": None, "id_cliente": ""}
    }
    taxis.append(new_taxi)
    save_taxis(taxis)

    return jsonify({'message': f'Taxi {taxi_id} registered successfully'}), 201

@app.route('/deregister_taxi/<int:taxi_id>', methods=['DELETE'])
def deregister_taxi(taxi_id):
    """Endpoint to deregister a taxi."""
    taxis = load_taxis()

    # Check if taxi exists
    taxi = next((taxi for taxi in taxis if taxi['id'] == taxi_id), None)
    if not taxi:
        return jsonify({'error': f'Taxi {taxi_id} not found'}), 404

    # Remove the taxi
    taxis.remove(taxi)
    save_taxis(taxis)

    return jsonify({'message': f'Taxi {taxi_id} deregistered successfully'}), 200

@app.route('/is_registered/<int:taxi_id>', methods=['GET'])
def is_registered(taxi_id):
    """Endpoint to check if a taxi is registered."""
    taxis = load_taxis()
    if any(taxi['id'] == taxi_id for taxi in taxis):
        return jsonify({'registered': True}), 200
    return jsonify({'registered': False}), 404

if __name__ == '__main__':
    # Generar certificados si no existen
    generate_certificates()

    # Ensure HTTPS by running the app with SSL certificates
    app.run(host='0.0.0.0', port=5002, ssl_context=('cert.pem', 'key.pem'))