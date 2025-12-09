import pika
import xml.etree.ElementTree as ET


RABBIT_HOST = "murabbitmq.ghhsdadqfycscha9.northcentralus.azurecontainer.io"
RABBIT_USER = "jdkyc"
RABBIT_PASS = "jdkyc"
STEP0_QUEUE = "jdkyc_STEP0_QUEUE"
STEP1_EXCHANGE = "STEP1_WORK_EXCHANGE"
STUDENT_B_PAWPRINT = "dwjq38"

def parse_step0_message(body_bytes):
    """Parse <Add> or <Multiply> message and compute the integer result."""
    root = ET.fromstring(body_bytes.decode())

    if root.tag == "Add":
        nums = [int(op.text) for op in root.findall("Operand")]
        return sum(nums)

    elif root.tag == "Multiply":
        nums = [int(op.text) for op in root.findall("Operand")]
        result = 1
        for n in nums:
            result *= n
        return result
    else:
        raise ValueError(f"Unexpected message type: {root.tag}")

def build_factorial_message(n):
    """Return <Factorial><Operand>N</Operand></Factorial> as bytes."""
    root = ET.Element("Factorial")
    op = ET.SubElement(root, "Operand")
    op.text = str(n)
    return ET.tostring(root, encoding="utf-8")




def main():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=5672,    
        credentials=creds
    )


    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=STEP0_QUEUE, durable=True)


    print("Running...")

    method_frame, header_frame, body = channel.basic_get(
        queue=STEP0_QUEUE,
        auto_ack=False
    )

    if method_frame:
        print(f"Received message: {body.decode()}")
        result = parse_step0_message(body)
        print(f"Computed result = {result}")
        factorial_msg = build_factorial_message(result)
        channel.exchange_declare(
            exchange=STEP1_EXCHANGE,
            exchange_type="direct",
            durable=True
        )
        channel.basic_publish(
            exchange=STEP1_EXCHANGE,
            routing_key=STUDENT_B_PAWPRINT,
            body=factorial_msg
        )
        print(f"Sent factorial message to {STUDENT_B_PAWPRINT}")
        channel.basic_ack(method_frame.delivery_tag)
    else:
        print("No message returned from the queue.")
    connection.close()
if __name__ == "__main__":
    main()





