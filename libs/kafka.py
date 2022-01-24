import os
import time
from abc import abstractclassmethod

import yaml

from libs import utils
from libs.base.containers import Container
from libs.base.controllers import Controller
from libs.base.executers import Executer
from libs.utils import NodeManager


class ZookeeperContainer(Container):
    """
    Zookeeperのコンテナ情報を保持するクラス
    """

    def __init__(self, name: str, configs: dict):
        configs['image'] = 'confluentinc/cp-zookeeper:5.5.6'
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
        configs['image'] = 'confluentinc/cp-kafka:5.5.6'
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


class KafkaClientContainer(Container):
    SERVICE: str
    CLIENT_COMMAND: str
    REQUIRE_CONFIGS: list
    ARBITRARY_CONFIGS: list

    def __init__(self, name: str, configs: dict):
        # configとresultの情報を設定
        self._config_info = {
            'config_dir': f'/tmp/{name}/configs',
            'config_filename': f'{self.__class__.SERVICE}_config.yml',
            'sinet_config_filename': '.sinetstream_config.yml'
        }
        self._result_info = {
            'result_dir': f'/tmp/{name}/results'
        }
        # コンテナ情報設定
        configs['image'] = 'todoroki182814/dms-client'
        if 'networks' not in configs.keys():
            configs['networks'] = ['kafka-network']
        configs['volumes'] = [
            {
                'type': 'bind',
                'source': self._config_info['config_dir'],
                'target': '/code/configs'
            },
            {
                'type': 'bind',
                'source': self._result_info['result_dir'],
                'target': '/code/results'
            }
        ]
        configs['command'] = self.__class__.CLIENT_COMMAND

        # paramsをconfigsから除き, コンストラクタを実行
        params = configs.pop('params')
        super().__init__(name, **configs)

        # paramsから実行に必要なconfigを作成
        self._set_configs(params)
        self._convert_configs()

    def _set_configs(self, params: dict):
        """
        sinetstreamで使用するパラメータとそれ以外のパラメータを分離する
        """
        # sinetstream以外の設定を作成
        self._configs = {
            'service': self.__class__.SERVICE,
            'name': self.name
        }
        for config_name in self.__class__.REQUIRE_CONFIGS:
            if config_name in params.keys():
                self._configs[config_name] = params.pop(config_name)
            else:
                raise RuntimeError(
                    f'Please sepcify {config_name} in {self.name}')
        for config_name in self.__class__.ARBITRARY_CONFIGS:
            if config_name in params.keys():
                self._configs[config_name] = params.pop(config_name)

        # sinetstreamの設定を作成
        params['type'] = 'kafka'
        params['value_type'] = 'text'
        self._sinet_configs = {self.__class__.SERVICE: params}

    @abstractclassmethod
    def _convert_configs(self):
        pass

    def to_swarm(self):
        # 終了後に再起動しないようにエラー時のみ再起動するよう設定
        swarm = super().to_swarm()
        restart_policy = {
            'condition': 'on-failure'
        }
        swarm[self.name]['deploy'].update({'restart_policy': restart_policy})
        return swarm

    def pre_up_process(self):
        # コンフィグファイルの作成
        config_file = os.path.join(
            self.home_dir, self._config_info['config_filename'])
        with open(config_file, 'w') as f:
            yaml.safe_dump(self._configs, f, sort_keys=False)
        sinet_config_file = os.path.join(
            self.home_dir, self._config_info['sinet_config_filename'])
        with open(sinet_config_file, 'w') as f:
            yaml.safe_dump(self._sinet_configs, f, sort_keys=False)

        # コンフィグファイルの転送
        self.node.sftp_put(config_file, os.path.join(
            self._config_info['config_dir'], self._config_info['config_filename']))
        self.node.sftp_put(sinet_config_file, os.path.join(
            self._config_info['config_dir'], self._config_info['sinet_config_filename']))

    def collect_results(self):
        ls_res, _ = self.node.ssh_exec(f'ls {self._result_info["result_dir"]}')
        for f in ls_res:
            self.node.sftp_get(os.path.join(
                self._result_info['result_dir'], f), os.path.join(self.home_dir, f))


class KafkaPubContainer(KafkaClientContainer):
    SERVICE = 'publisher'
    CLIENT_COMMAND = 'python publisher.py'
    REQUIRE_CONFIGS = ['duration', 'number', 'message_size']
    ARBITRARY_CONFIGS = ['message_rate', 'wait_time']

    def _convert_configs(self):
        self._configs['duration'] = utils.change_time_to_sec(
            self._configs['duration'])
        self._configs['message_size'] = utils.change_size_to_byte(
            self._configs['message_size'])
        if 'message_rate' in self._configs.keys():
            self._configs['message_rate'] = utils.change_size_to_byte(
                self._configs['message_rate'])
        if 'wait_time' in self._configs.keys():
            self._configs['wait_time'] = utils.change_time_to_sec(
                self._configs['wait_time'])


class KafkaSubContainer(KafkaClientContainer):
    SERVICE = 'subscriber'
    CLIENT_COMMAND = 'python subscriber.py'
    REQUIRE_CONFIGS = ['duration', 'number']
    ARBITRARY_CONFIGS = ['record_message', 'wait_time']

    def _convert_configs(self):
        self._configs['duration'] = utils.change_time_to_sec(
            self._configs['duration'])
        if 'wait_time' in self._configs.keys():
            self._configs['wait_time'] = utils.change_time_to_sec(
                self._configs['wait_time'])


class KafkaController(Controller):
    BROKER_SERVICE = 'kafka-broker'
    PUBLISHER_SERVICE = 'kafka-publihser'
    SUBSCRIBER_SERVICE = 'kafka-subscriber'

    def __init__(self, node_manager: NodeManager, executer: Executer, systems: dict, root_dir: str):
        super().__init__(node_manager, executer, systems, root_dir)

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
            if 'duration' not in configs['params'].keys():
                configs['params']['duration'] = systems['duration']
            self._publisher.append(KafkaPubContainer(name, configs))

        # Subscriberのコンテナ情報の作成
        sub_info = systems['subscriber']
        for name, configs in sub_info.items():
            if 'duration' not in configs['params'].keys():
                configs['params']['duration'] = systems['duration']
            self._subscriber.append(KafkaSubContainer(name, configs))

        self._containers.extend(self._broker)
        self._containers.extend(self._publisher)
        self._containers.extend(self._subscriber)

        # 作成するトピックの情報
        if "topics" in systems["broker"]:
            self._topic_info = systems["broker"]["topics"]

    def create_topics(self):
        brokers = self._topic_info["brokers"]
        topic_list = self._topic_info["list"]
        topic_create_cmd = ""
        topic_describe_cmd = ""
        for topic in topic_list:
            topic_name, partitions, replication_factor = topic.split(':')
            cmd = f"kafka-topics --bootstrap-server {','.join(brokers)} --topic {topic_name} --partitions {partitions} --replication-factor {replication_factor} --create"
            if topic_create_cmd != "":
                topic_create_cmd += " && "
            topic_create_cmd += cmd

            cmd = f"kafka-topics --bootstrap-server {','.join(brokers)} --topic {topic_name} --describe"
            if topic_describe_cmd != "":
                topic_describe_cmd += " && "
            topic_describe_cmd += cmd

        for container in self._broker:
            if isinstance(container, KafkaContainer):
                kafka = container
                break
        self._executre.create_topics(kafka, topic_create_cmd, topic_describe_cmd)
