import os
import re
import yaml

from libs.base.containers import Container
from libs.base.controllers import Controller
from libs.base.executers import Executer
from libs.utils import NodeManager


class ZookeeperContainer(Container):
    """
    Zookeeperのコンテナ情報を保持するクラス
    """

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

    def collect_results(self):
        # 収集処理無し
        pass


class KafkaContainer(Container):
    """
    Kafkaのコンテナ情報を保持するクラス
    """

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

    def collect_results(self):
        # 収集処理無し
        pass


class KafkaClientContainer(Container):
    SERVICE: str
    CLIENT_COMMAND: str
    REQUIRE_CONFIGS: list
    ARBITRARY_CONFIGS: list

    def __init__(self, name: str, configs: dict):
        super().__init__(name, **configs)
        self.image = 'todoroki182814/dms-client'
        # configとresultの情報を設定
        self._config_info = {
            'config_dir': f'/tmp/{self.name}/configs',
            'config_filename': f'{self.__class__.SERVICE}_config.yml',
            'sinet_config_filename': '.sinetstream_config.yml'
        }
        self._result_info = {
            'result_dir': f'/tmp/{self.name}/results'
        }
        self.volumes = [
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
        if self.networks is None:
            self.networks = ['kafka-network']
        # 実行するコマンドの設定
        self.command = self.__class__.CLIENT_COMMAND
        # configの設定
        params = configs['params']
        self._set_configs(params)

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

        # 実行時間の指定方法がs, m, hでなければエラーを出す
        if re.search('h|m|s', self._configs['duration']) is None:
            raise ValueError(
                'Please specify "s" or "m" or "h" for the duration')

        # sinetstreamの設定を作成
        params['type'] = 'kafka'
        params['value_type'] = 'text'
        self._sinet_configs = {self.__class__.SERVICE: params}

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
    """
    Kafkaのコンテナ情報を保持するクラス
    """
    SERVICE = 'publisher'
    CLIENT_COMMAND = 'python publisher.py'
    REQUIRE_CONFIGS = ['duration', 'number', 'message_size']
    ARBITRARY_CONFIGS = ['message_rate']


class KafkaSubContainer(KafkaClientContainer):
    SERVICE = 'subscriber'
    CLIENT_COMMAND = 'python subscriber.py'
    REQUIRE_CONFIGS = ['duration', 'number']
    ARBITRARY_CONFIGS = ['record_message']


class KafkaController(Controller):
    BROKER_SERVICE = 'kafka-broker'
    PUBLISHER_SERVICE = 'kafka-publihser'
    SUBSCRIBER_SERVICE = 'kafka-subscriber'

    def __init__(self, node_manager: NodeManager, executer: Executer, systems: dict, root_dir: str):
        super().__init__(node_manager, executer, systems, root_dir)

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
            if 'duration' not in configs['params'].keys():
                configs['params']['duration'] = systems['duration']
            self._publisher.append(KafkaPubContainer(name, configs))
        self._containers.extend(self._publisher)

        # Subscriberのコンテナ情報の作成
        sub_info = systems['Subscriber']
        for name, configs in sub_info['containers'].items():
            if 'duration' not in configs['params'].keys():
                configs['params']['duration'] = systems['duration']
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
