import os
import time
from abc import abstractclassmethod

import yaml

from libs import utils
from libs.base.containers import Container
from libs.base.controllers import Controller
from libs.base.executers import Executer
from libs.clients import ClientContainer
from libs.utils import NodeManager


class ZookeeperContainer(Container):
    """
    Zookeeperのコンテナ情報を保持するクラス
    """

    def __init__(self, name: str, configs: dict):
        configs['image'] = 'confluentinc/cp-zookeeper:6.2.4'
        if 'networks' not in configs.keys():
            configs['networks'] = ['kafka-network']
        if 'volumes' not in configs.keys():
            configs['volumes'] = [
                {
                    'type': 'bind',
                    'source': f'/tmp/{name}/log',
                    'target': '/var/lib/zookeeper/log'
                },
                {
                    'type': 'bind',
                    'source': f'/tmp/{name}/data',
                    'target': '/var/lib/zookeeper/data'
                }
            ]
        super().__init__(name, **configs)

    def pre_up_process(self):
        # 事前処理無し
        pass

    def collect_results(self):
        # 収集処理無し
        pass


class KafkaContainer(Container):
    """
    Kafkaのコンテナ情報を保持するクラス
    """

    def __init__(self, name: str, configs: dict):
        configs['image'] = 'confluentinc/cp-kafka:6.2.4'
        if 'networks' not in configs.keys():
            configs['networks'] = ['kafka-network']
        if 'volumes' not in configs.keys():
            configs['volumes'] = [
                {
                    'type': 'bind',
                    'source': f'/tmp/{name}/data',
                    'target': '/var/lib/kafka/data'
                },
            ]
        super().__init__(name, **configs)

    def pre_up_process(self):
        # 事前処理無し
        pass

    def collect_results(self):
        # 収集処理無し
        pass


class KafkaController(Controller):
    BROKER = "kafka"
    BROKER_SERVICE = 'kafka-broker'
    PUBLISHER_SERVICE = 'kafka-publihser'
    SUBSCRIBER_SERVICE = 'kafka-subscriber'


    def create_containers(self, systems):
        # Brokerのコンテナ情報の作成
        zoo_info = systems['broker']['zookeeper']
        for name, configs in zoo_info.items():
            self._broker.append(ZookeeperContainer(name, configs))
        kafka_info = systems['broker']['kafka']
        for name, configs in kafka_info.items():
            self._broker.append(KafkaContainer(name, configs))

        # Publisherのコンテナ情報の作成
        pub_info = systems['publisher']
        for name, configs in pub_info.items():
            configs['networks'] = self._broker[0].networks
            self._publisher.append(ClientContainer(name, configs))

        # Subscriberのコンテナ情報の作成
        sub_info = systems['subscriber']
        for name, configs in sub_info.items():
            configs['networks'] = self._broker[0].networks
            self._subscriber.append(ClientContainer(name, configs))

        self._containers.extend(self._broker)
        self._containers.extend(self._publisher)
        self._containers.extend(self._subscriber)

    def create_topic_info(self, systems: dict):
        # トピックの情報を作成する
        if "topics" in systems["broker"]:
            topic_info = systems["broker"]["topics"]
            self._topic_create_cmd = ""
            self._build_topic_create_command(topic_info)
            self._topic_describe_cmd = ""
            self._build_topic_describe_command(topic_info)

    def _build_topic_create_command(self, topic_info):
        # トピック作成コマンドの組み立て
        brokers = topic_info["brokers"]
        topic_list = topic_info["list"]
        for topic in topic_list:
            topic_name, partitions, replication_factor = topic.split(':')
            cmd = f"kafka-topics --bootstrap-server {','.join(brokers)} --topic {topic_name} --partitions {partitions} --replication-factor {replication_factor} --create"
            if self._topic_create_cmd != "":
                self._topic_create_cmd += " && "
            self._topic_create_cmd += cmd

    def _build_topic_describe_command(self, topic_info):
        # トピック詳細コマンドの組み立て
        brokers = topic_info["brokers"]
        topic_list = topic_info["list"]
        for topic in topic_list:
            topic_name, partitions, replication_factor = topic.split(':')
            cmd = f"kafka-topics --bootstrap-server {','.join(brokers)} --topic {topic_name} --describe"
            if self._topic_describe_cmd != "":
                self._topic_describe_cmd += " && "
            self._topic_describe_cmd += cmd

    def create_topics(self):
        # クラスターの同期時間
        sec = len(self._broker) * 3
        print(f"Waiting for cluster synchronization.({sec} sec)")
        time.sleep(sec)  
        for container in self._broker:
            if isinstance(container, KafkaContainer):
                kafka = container
                break
        self._executre.exec_command_in_container(kafka, self._topic_create_cmd)

    def describe_topics(self):
        for container in self._broker:
            if isinstance(container, KafkaContainer):
                kafka = container
                break
        self._executre.exec_command_in_container(
            kafka, self._topic_describe_cmd)
