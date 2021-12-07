import os
import re
import yaml

from libs.base.containers import Container
from libs.base.controllers import Controller
from libs.base.executers import Executer
from libs.utils import NodeManager


class ZookeeperContainer(Container):
    def __init__(self, name: str, configs: dict):
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

    def pre_up_process(self):
        # 事前処理無し
        pass


class KafkaContainer(Container):
    def __init__(self, name: str, configs: dict):
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

    def pre_up_process(self):
        # 事前処理無し
        pass


class KafkaClientContainer(Container):
    SERVICE: str
    CLIENT_COMMAND: str
    CONFIG_LIST: list

    def __init__(self, name: str, configs: dict):
        super().__init__(name, **configs)
        self.image = 'todoroki182814/dms-client'
        self.volumes = [
            {
                'type': 'bind',
                'source': f'/tmp/{self.name}/configs',
                'target': '/code/configs'
            },
            {
                'type': 'bind',
                'source': f'/tmp/{self.name}/results',
                'target': '/code/results'
            }
        ]
        if self.networks is None:
            self.networks = ['kafka-network']
        self.command = self.__class__.CLIENT_COMMAND

        self._config_info = {
            'config_dir': f'/tmp/{self.name}/configs',
            'config_filename': f'{self.__class__.SERVICE}_config.yml',
            'sinet_config_filename': '.sinetstream_config.yml'
        }
        params = configs['params']
        self._set_configs(params)

    def _set_configs(self, params: dict):
        # sinetstream以外の設定を作成
        self._configs = {
            'service': self.__class__.SERVICE,
            'name': self.name
            }
        for config_name in self.__class__.CONFIG_LIST:
            if config_name in params.keys():
                self._configs[config_name] = params.pop(config_name)

        duration = params.pop('duration')
        if re.search('h|m|s', duration) is None:
            raise ValueError(
                'Please specify "s" or "m" or "h" for the duration')
        self._configs['duration'] = duration

        # sinetstreamの設定を作成
        params['type'] = 'kafka'
        params['value_type'] = 'text'
        self._sinet_configs = {self.__class__.SERVICE: params}

    def to_swarm(self):
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


class KafkaPubContainer(KafkaClientContainer):
    SERVICE = 'publisher'
    CLIENT_COMMAND = 'python publisher.py'
    CONFIG_LIST = ['number', 'message_size']


class KafkaSubContainer(KafkaClientContainer):
    SERVICE = 'subscriber'
    CLIENT_COMMAND = 'python subscriber.py'
    CONFIG_LIST = ['number']


class KafkaController(Controller):
    def __init__(self, node_manager: NodeManager, executer: Executer, systems: dict, home_dir: str):
        super().__init__(node_manager, executer, systems, home_dir)

        # Brokerのコンテナ情報の作成
        zoo_info = systems['Broker']['zookeeper']
        for name, configs in zoo_info['containers'].items():
            self._broker.append(ZookeeperContainer(name, configs))
        kafka_info = systems['Broker']['kafka']
        for name, configs in kafka_info['containers'].items():
            self._broker.append(KafkaContainer(name, configs))
        self._containers.extend(self._broker)

        # Publisherのコンテナ情報の作成
        pub_info = systems['Publisher']
        for name, configs in pub_info['containers'].items():
            self._publisher.append(KafkaPubContainer(name, configs))
        self._containers.extend(self._publisher)

        # Subscriberのコンテナ情報の作成
        sub_info = systems['Subscriber']
        for name, configs in sub_info['containers'].items():
            self._subscriber.append(KafkaSubContainer(name, configs))
        self._containers.extend(self._subscriber)

        # 作成するトピックの情報を作成
        self._topics = []
        if 'topics' in systems['Broker']:
            for topic_info in systems['Broker']['topics']:
                topic_name, partitions, replication_factor = topic_info.split(
                    ':')
                self._topics.append(topic={
                    'topic': topic_name,
                    'partitions': partitions,
                    'replication-factor': replication_factor
                })

    def clean(self):
        print(f"remove kafka services")
        self._executre.down_containers(
            self._broker, self._node_manager, 'kafka-broker')
        self._executre.down_containers(
            self._broker, self._node_manager, 'kafka-publisher')
        self._executre.down_containers(
            self._broker, self._node_manager, 'kafka-subscriber')
        super().clean()

    def deploy_broker(self):
        # brokerコンテナを展開
        print('------------Deploy broker containers------------')
        print(f"create kafka-broker")
        self._executre.up_containers(
            self._broker, self._node_manager, 'kafka-broker')

    def deploy_publisher(self):
        print('------------Deploy publisher containers------------')
        print(f"create kafka-publisher")
        self._executre.up_containers(
            self._publisher, self._node_manager, 'kafka-publisher')

    def deploy_subscriber(self):
        print('------------Deploy subscriber containers------------')
        print(f"create kafka-subscriber")
        self._executre.up_containers(
            self._subscriber, self._node_manager, 'kafka-subscriber')
