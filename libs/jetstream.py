import glob
import os


from libs.base.containers import Container
from libs.base.controllers import Controller

JETSTREAM_NETWORK = "jetstream-network"


class JetStreamContainer(Container):

    def __init__(self, name, configs: dict):
        configs['image'] = 'nats:2.8-alpine'
        if 'networks' not in configs.keys():
            configs['networks'] = [JETSTREAM_NETWORK]
        if 'volumes' not in configs.keys():
            configs['volumes'] = [
                {
                    'type': 'bind',
                    'source': f'/tmp/{name}/data',
                    'target': '/tmp/data'
                },
            ]
        configs["volumes"].append(
            {
                'type': 'bind',
                'source': f'/tmp/{name}/configs',
                'target': f'/tmp/configs'
            }
        )
        super().__init__(name, **configs)

    def pre_up_process(self):
        file_list = glob.glob(os.path.join(self.get_config_path(), "*"))
        for file in file_list:
            self.node.sftp_put(file, '/tmp/configs')

    def collect_results(self):
        pass


class JetStreamController(Controller):
    BROKER_SERVICE = "jetstream-broker"
    PUBLISHER_SERVICE = "jetstream-publisher"
    SUBSCRIBER_SERVICE = "jetstream-subscriber"

    def create_containers(self, systems: dict):
        jetstream_info = systems['broker']['jetstream']
        for name, configs in jetstream_info.items():
            self._broker.append(JetStreamContainer(name, configs))

        self._containers.extend(self._broker)

    def create_topic_info(self, systems):
        pass

    def create_topics(self):
        pass

    def describe_topics(self):
        pass

    