import pika
import time
import os
import json

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = "image_processing_tasks"

def get_connection(retries=10, delay=5):
    """Try to connect to RabbitMQ, retrying if not ready."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
    for attempt in range(retries):
        try:
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as e:
            print(f"RabbitMQ not ready (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception("Could not connect to RabbitMQ after several attempts")


def publish_task(task_data):
    """Publish a task message to RabbitMQ."""
    try:
        connection = get_connection()
        channel = connection.channel()
        
        # Declare queue with durability
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        
        # Publish message with persistence
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(task_data),
            properties=pika.BasicProperties(
                delivery_mode=2  # make message persistent
            )
        )
        connection.close()
        return True
    except Exception as e:
        print(f"Error publishing task: {e}")
        return False

def setup_consumer(callback):
    """Set up a RabbitMQ consumer with the given callback function."""
    connection = get_connection()
    channel = connection.channel()
    
    # Declare queue with durability
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    # Set prefetch count to 1 to ensure fair distribution
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback
    )
    
    return connection, channel
