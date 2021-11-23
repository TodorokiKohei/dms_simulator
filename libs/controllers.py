import os
from abc import ABCMeta, abstractclassmethod, abstractmethod

from libs.containers import KafkaContainer, ZookeeperContainer


class Controller(metaclass=ABCMeta):
    def __init__(self, node_manager, executer):
        self._node_manager = node_manager
        self._executre = executer

        self._containers = []
        self._broker = []
        self._publisher = []
        self._subscriber = []

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def clean(self):
        pass

    @abstractclassmethod
    def deploy_broker(self):
        pass

    @abstractclassmethod
    def deploy_publisher(self):
        pass

    @abstractclassmethod
    def deploy_subscriber(self):
        pass


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

    def init(self):
        for node in self._node_manager.get_nodes():
            node.init()
        self._executre.init(self._containers, self._node_manager)

    def clean(self):
        self._executre.clean(self._containers, self._node_manager)

    def deploy_broker(self):
        # brokerコンテナを展開
        print('------------Deploy broker containers------------')
        self._executre.up_containers(
            self._broker, self._node_manager, 'kafka-broker')

    def deploy_publisher(self):
        print('------------Deploy publisher containers------------')
        pass

    def deploy_subscriber(self):
        print('------------Deploy subscriber containers------------')
        pass

    def remove(self):
        self._executre.down_containers(
            self._broker, self._node_manager, 'kafka-broker')
