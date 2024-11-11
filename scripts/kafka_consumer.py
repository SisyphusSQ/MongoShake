from confluent_kafka import Consumer, KafkaException, KafkaError

conf = {
    'bootstrap.servers': 'broker1:9092,broker2:9092,broker3:9092',
    'group.id': 'tttt',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
topic = 'Mongoshake-Test'
consumer.subscribe([topic])


def main():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.partition()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print("Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}".format(
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    msg.key(),
                    msg.value()))
    except KeyboardInterrupt:
        print("Received keyboard interrupt, exiting...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
