from libs.base.containers import Container
from libs.base.controllers import Controller


class ZookeeperContainer(Container):
    def __init__(self, name, configs):
        super().__init__(name, **configs)
        self.image = "confluentinc/cp-zookeeper:5.5.6"
        if self.volumes is None:
            self.volumes = [
                {
                    'type': 'bind',
                    'source': f'/tmp/{self.name}/log',
                    'target': '/var/lib/zookeeper/log'
                },
                {
                    'type': 'bind',
                    'source': f'/tmp/{self.name}/data',
                    'target': '/var/lib/zookeeper/data'
                }
            ]
        if self.networks is None:
            self.networks = ['kafka-network']


class KafkaContainer(Container):
    def __init__(self, name, configs):
        super().__init__(name, **configs)
        self.image = "confluentinc/cp-kafka:5.5.6"
        if self.volumes is None:
            self.volumes = [
                {
                    'type': 'bind',
                    'source': f'/tmp/{self.name}/data',
                    'target': '/var/lib/kafka/data'
                },
            ]
        if self.networks is None:
            self.networks = ['kafka-network']


class KafkaController(Controller):
    def __init__(self, node_manager, executer, systems):
        super().__init__(node_manager, executer)

        # Brokerのコンテナ情報の作成
        zoo_info = systems['Broker']['zookeeper']
        for name, configs in zoo_info['containers'].items():
            self._broker.append(ZookeeperContainer(name, configs))
        kafka_info = systems['Broker']['kafka']
        for name, configs in kafka_info['containers'].items():
            self._broker.append(KafkaContainer(name, configs))
        self._containers.extend(self._broker)

        # コンテナを展開するノードを設定
        for container in self._containers:
            node = self._node_manager.get_match_node(container.node_name)
            container.node = node

        # 作成するトピックの情報を作成
        if 'topics' in systems['Broker']:
            self._topics = []
            for topic_info in systems['Broker']['topics']:
                topic_name, partitions, replication_factor = topic_info.split(
                    ':')
                self._topics.append(topic={
                    'topic': topic_name,
                    'partitions': partitions,
                    'replication-factor': replication_factor
                })

    def clean(self):
        print(f"remove kafka-broker")
        self._executre.down_containers(
            self._broker, self._node_manager, 'kafka-broker')
        super().clean()

    def deploy_broker(self):
        # brokerコンテナを展開
        print('------------Deploy broker containers------------')
        print(f"create kafka-broker")
        self._executre.up_containers(
            self._broker, self._node_manager, 'kafka-broker')

    def deploy_publisher(self):
        print('------------Deploy publisher containers------------')
        

    def deploy_subscriber(self):
        print('------------Deploy subscriber containers------------')
