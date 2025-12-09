import time
import pika
import pika.exceptions
import xml.etree.ElementTree as ET


RABBIT_HOST = "murabbitmq.ghhsdadqfycscha9.northcentralus.azurecontainer.io"
RABBIT_USER = "jdkyc"
RABBIT_PASS = "jdkyc"
STEP0_QUEUE = "dwjq38_STEP0"
STEP1_EXCHANGE = "STEP1_WORK_EXCHANGE"
STUDENT_B_PAWPRINT = "dwjq38"

def parse_step0_message(body_bytes):
    """Parse <Add> or <Multiply> message and compute the integer result."""
    root = ET.fromstring(body_bytes.decode())

    if root.tag == "Add":
        nums = [int(op.text) for op in root.findall("Operand")]
        return sum(nums)

    elif root.tag == "Mult":
        nums = [int(op.text) for op in root.findall("Operand")]
        result = 1
        for n in nums:
            result *= n
        return result
    
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




def on_message(channel, method, properties, body):
    try:
        print(f"Received message: {body.decode()}")
        result = parse_step0_message(body)
        print(f"Computed result = {result}")
        factorial_msg = build_factorial_message(result)

        # Ensure exchange exists (will raise if type mismatches)
        try:
            channel.exchange_declare(
                exchange=STEP1_EXCHANGE,
                exchange_type="direct",
                durable=True,
            )
        except pika.exceptions.ChannelClosedByBroker as ex:
            print(f"Exchange declare failed: {ex}")
            # If the channel was closed by the broker, we cannot continue on this channel.
            # Let the outer consumer/reconnect logic handle reopening.
            raise

        channel.basic_publish(
            exchange=STEP1_EXCHANGE,
            routing_key=STUDENT_B_PAWPRINT,
            body=factorial_msg,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"Sent factorial message to {STUDENT_B_PAWPRINT}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as ex:
        print("Error processing message:", ex)
        try:
            if channel and getattr(channel, 'is_open', False):
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as nack_exc:
            print("Failed to nack message:", nack_exc)


def run_consumer():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=5672, credentials=creds)

    backoff = 1
    while True:
        connection = None
        channel = None
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=STEP0_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(queue=STEP0_QUEUE, on_message_callback=on_message, auto_ack=False)
            print("Consumer started for STEP0_QUEUE. Waiting for messages.")
            channel.start_consuming()
        except KeyboardInterrupt:
            print("Interrupted, shutting down consumer.")
            try:
                if channel and getattr(channel, 'is_open', False):
                    channel.stop_consuming()
            except Exception:
                pass
            try:
                if connection and getattr(connection, 'is_open', False):
                    connection.close()
            except Exception:
                pass
            break
        except pika.exceptions.AMQPConnectionError as conn_ex:
            print(f"Connection error, reconnecting in {backoff}s: {conn_ex}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as ex:
            print(f"Unexpected error in consumer loop: {ex}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            try:
                if connection and getattr(connection, 'is_open', False):
                    connection.close()
            except Exception:
                pass


if __name__ == "__main__":
    run_consumer()





