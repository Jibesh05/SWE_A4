import pika
import pika.exceptions
import xml.etree.ElementTree as ET

RABBIT_HOST = "murabbitmq.ghhsdadqfycscha9.northcentralus.azurecontainer.io"
RABBIT_USER = "dwjq38"
RABBIT_PASS = "dwjq38"
STEP1_QUEUE = "dwjq38_STEP1"
STEP2_EXCHANGE = "STEP2_WORK_EXCHANGE"
STUDENT_A_PAWPRINT = "jdkyc"
STUDENT_B_PAWPRINT = "dwjq38"

def parse_step1_message(body_bytes):
    """Parse <Factorial> message and compute the integer result."""
    root = ET.fromstring(body_bytes.decode())

    if root.tag == "Factorial":
        num = int(root.find("Operand").text)
        result = 1
        for i in range(1, num + 1):
            result *= i
        return result
    else:
        raise ValueError(f"Unexpected message type: {root.tag}")
    
def build_result_message(n):
    """Return <Output><Result>N</Result><StudentA>jdkyc</StudentA><StudentB>dwjq38</StudentB></Output> as bytes."""
    root = ET.Element("Output")
    res = ET.SubElement(root, "Result")
    res.text = str(n)
    student_a = ET.SubElement(root, "StudentA")
    student_a.text = STUDENT_A_PAWPRINT
    student_b = ET.SubElement(root, "StudentB")
    student_b.text = STUDENT_B_PAWPRINT
    return ET.tostring(root, encoding="utf-8")

def main():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=5672, credentials=creds)


def on_message(channel, method, properties, body):
    try:
        n = parse_step1_message(body)
        result_msg = build_result_message(n)

        # Try to declare the exchange. If an exchange with the same name but
        # a different type already exists the broker will close the channel
        # with a PRECONDITION_FAILED (406). Let that propagate so the outer
        # reconnect loop can re-establish a fresh channel.
        channel.exchange_declare(
            exchange=STEP2_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )

        channel.basic_publish(
            exchange=STEP2_EXCHANGE,
            routing_key="",
            body=result_msg,
            properties=pika.BasicProperties(delivery_mode=2),
        )

        channel.basic_ack(delivery_tag=method.delivery_tag)
        print("Processed and sent result message " + result_msg.decode() + "\n")
    except Exception as e:
        print(f"Error processing message: {e}")
        try:
            if channel and getattr(channel, 'is_open', False):
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                print("Channel is closed; cannot nack. Broker will requeue message.")
        except Exception as nack_exc:
            print(f"Failed to nack message: {nack_exc}")


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
            channel.queue_declare(queue=STEP1_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(queue=STEP1_QUEUE, on_message_callback=on_message, auto_ack=False)
            print("Consumer started for STEP1_QUEUE. Waiting for messages.")
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
            import time
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as ex:
            print(f"Unexpected error in consumer loop: {ex}")
            import time
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