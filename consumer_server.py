from kafka import KafkaConsumer
from json import loads


def consume():

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        client_id="sfc_crimes_consumer",
        auto_offset_reset='earliest',
        # enable_auto_commit=True,
        group_id='sfc_crimes_consumer_group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    consumer.subscribe("org.sfc.crimes")

    # TODO implement poll loop
    for message in consumer:
        print(f"Consumed message: {message}")

    consumer.close()


    # TODO print ech message to console

    pass


if __name__ == "__main__":

    try:
        consume()
    except KeyboardInterrupt as e:
        print("Keyboard interrupt by user, shutting down consumer server.")

