import time
from confluent_kafka import Producer


producer = Producer({'bootstrap.servers': 'localhost:29092'})

def produce_record_data(topic: str, record_id: int):
    try:
        while True:
            message = {"id": record_id}
            producer.produce(topic, key=str(record_id), value=str(message))
            producer.flush()
            print(f"Produced record: {message}")
            record_id += 1
            time.sleep(60)  # Wait for 1 minute before producing the next message
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()

if __name__=="__main__":
    
    KAFKA_TOPIC = "STREAMER_ID_TOPIC"

    producer.poll(0)
    metadata = producer.list_topics(topic=KAFKA_TOPIC)
    if KAFKA_TOPIC not in metadata.topics:
        producer.create_topics([KAFKA_TOPIC])

    produce_record_data(topic=KAFKA_TOPIC, record_id=100)

