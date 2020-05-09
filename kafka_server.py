import producer_server


def run_kafka_server():

    # TODO (done) get the json file path
    input_file = "./police-department-calls-for-service.json"

    # TODO (done) fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="org.sfc.crimes",
        bootstrap_servers="localhost:9092",
        client_id="sfc_crimes_producer"
    )
    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
