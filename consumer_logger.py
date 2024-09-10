from confluent_kafka import Consumer

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':55003'


def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': 'demo_logger'}
    consumer = Consumer(conf)
    try:
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Error receiving message: {msg.topic()}')
            else:
                print(msg.value().decode())
    except Exception as e:
        print(f'Uncaught exception: {e}')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
