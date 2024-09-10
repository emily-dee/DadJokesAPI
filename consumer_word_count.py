from confluent_kafka import Consumer

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':55003'


def main():
    joke_count = 0
    total_word_count = 0
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
                joke_count += 1
                joke = msg.value().decode()
                joke_list = joke.split(' ')
                word_count = len(joke_list)
                total_word_count += word_count
                avg_count = total_word_count / joke_count
                if joke_count % 5 == 0:
                    print(f'The average word count is: {avg_count}')


                print()
    except Exception as e:
        print(f'Uncaught exception: {e}')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
