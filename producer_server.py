from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO (done) we're generating a dummy data
    def generate_data(self):
        print("load input file")
        with open(self.input_file, "r") as f:
            json_data = json.loads(f.read())  # this fixes the input since it is an array of multiline json objects
            for line in json_data:
                print(f"send message {line}")
                message = self.dict_to_binary(line)
                # TODO send the correct data
                self.send(
                    topic="org.sfc.crimes",
                    value=message
                )
                # time.sleep(1)

    # TODO (done) fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode("utf-8")
