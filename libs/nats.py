

from libs.base.containers import Container
from libs.base.controllers import Controller
from libs.clients import ClientContainer


NATS_NETWORK = "nats-network"


class NatsContainer(Container):
    def __init__(self, name: str, configs: dict):
        configs['image'] = 'nats:2.8-alpine'
        if 'networks' not in configs.keys():
            configs['networks'] = [NATS_NETWORK]
        if 'volumes' not in configs.keys():
            configs['volumes'] = [
                {
                    'type': 'bind',
                    'source': f'/tmp/{name}/configs',
                    'target': '/tmp/configs'
                }
            ]
        if 'config_info' not in configs.keys():
            configs['config_info'] = [
                {
                    'file': '*',
                    'to': f'/tmp/{name}/configs'
                }
            ]
        super().__init__(name, **configs)

    def pre_up_process(self):
        self.transfer_configs()

    def collect_results(self):
        pass


class NatsController(Controller):
    BROKER = "nats"
    BROKER_SERVICE = "nats-broker"
    PUBLISHER_SERVICE = "nats-publisher"
    SUBSCRIBER_SERVICE = "nats-subscriber"

    def create_containers(self, systems: dict):
        nats_info = systems['broker']['nats']

        for name, configs in nats_info.items():
            self._broker.append(NatsContainer(name, configs))
        for name, configs in systems['publisher'].items():
            configs['networks'] = self._broker[0].networks
            self._publisher.append(ClientContainer(name, configs))
        for name, configs in systems['subscriber'].items():
            configs['networks'] = self._broker[0].networks
            self._subscriber.append(ClientContainer(name, configs))

        self._containers.extend(self._broker)
        self._containers.extend(self._publisher)
        self._containers.extend(self._subscriber)

    def create_topic_info(self, systems:dict):
        pass

    def create_topics(self):
        pass

    def describe_topics(self):
        pass
