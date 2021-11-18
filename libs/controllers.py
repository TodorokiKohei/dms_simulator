from abc import ABCMeta, abstractclassmethod

from libs.containers import KafkaContainer, ZookeeperContainer


class Controller(metaclass=ABCMeta):
    def __init__(self, executer):
        self._executre = executer


class KafkaController(Controller):
    def __init__(self, executer, systems):
        super().__init__(executer)
        self.zoo = []
        zoo_info = systems['Broker']['zookeeper']
        for name, container_info in zoo_info['containers'].items():
            self.zoo.append(ZookeeperContainer(
                name, container_info['node'], container_info['configs']
                ))
        self.kafka = []
        kafka_info = systems['Broker']['kafka']
        for name, container_info in kafka_info['containers'].items():
            self.kafka.append(KafkaContainer(
                name, container_info['node'], container_info['configs']
                ))
    

