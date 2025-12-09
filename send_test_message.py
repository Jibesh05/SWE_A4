import pika

HOST = "murabbitmq.ghhsdadqfycscha9.northcentralus.azurecontainer.io"
USER = "jdkyc"
PASS = "jdkyc"
QUEUE = "jdkyc_STEP0_QUEUE"

creds = pika.PlainCredentials(USER, PASS)
params = pika.ConnectionParameters(host=HOST,port = 5672, credentials=creds)

conn = pika.BlockingConnection(params)
ch = conn.channel()

# Example test message
msg = b"<Multiply><Operand>5</Operand><Operand>4</Operand></Multiply>"

# Publish to your STEP0 queue
ch.basic_publish(
    exchange="",
    routing_key=QUEUE,
    body=msg
)

print("Sent test message!")
conn.close()
