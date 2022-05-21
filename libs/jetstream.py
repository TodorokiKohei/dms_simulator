import glob
import os

from libs.base.containers import Container
from libs.base.controllers import Controller

JETSTREAM_NETWORK = "jetstream-network"


class NatsBox(Container):
    SERVICE = 'nats-box'

    def __init__(self, name, server):
        self._server = server
        self._config_source = f'/tmp/{name}/configs'
        self._config_target = '/tmp/configs'
        configs = {}
        configs['image'] = 'natsio/nats-box'
        configs['volumes'] = [
            {
                'type': 'bind',
                'source': self._config_source,
                'target': self._config_target
            }
        ]
        configs['command'] = '-c "while :; do sleep 10; done"'
        super().__init__(name, **configs)

        self._stream_configs = []

    def set_stream_configs(self, stream_configs: list):
        self._stream_configs.extend(stream_configs)

    def build_command_to_create_stream(self):
        command = 'sh -c "'
        is_first = True
        for stream_config in self._stream_configs:
            stream_name = stream_config['name']
            stream_file = os.path.join(self._config_target, stream_config['file'])
            cmd = f'nats -s nats://{self._server} str add {stream_name} --config {stream_file}'
            for consumer_file in stream_config['consumer']:
                consumer_file = os.path.join(self._config_target, consumer_file)
                cmd += f' && nats -s nats://{self._server} con add {stream_name} --config {consumer_file}'
            if is_first:
                command += cmd
                is_first = False
            else:
                command += ' && ' + cmd
        return command + '"'

    def build_command_to_describe_stream(self):
        command = 'sh -c "'
        is_first = True
        for stream_config in self._stream_configs:
            stream_name = stream_config['name']
            cmd = f'nats -s nats://{self._server} str info {stream_name} -j'
            if is_first:
                command += cmd
                is_first = False
            else:
                command += ' && ' + cmd
        return command + '"'

    def pre_up_process(self):
        self.create_volume_dir()
        for configs in self._stream_configs:
            file_list = glob.glob(os.path.join(
                self.home_dir, '*'))
            for file in file_list:
                remote_filename = os.path.join(self._config_source, os.path.basename(file))
                self.node.sftp_put(file, remote_filename)

    def collect_results(self):
        pass


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
                {
                    'type': 'bind',
                    'source': f'/tmp/{name}/configs',
                    'target': '/tmp/configs'
                }
            ]
        if 'configs' not in configs.keys():
            self._configs = [
                {
                    'file': '*',
                    'to': f'/tmp/{name}/configs'
                }
            ]
        else:
            self._configs = configs.pop('configs')
        super().__init__(name, **configs)

    def pre_up_process(self):
        for configs in self._configs:
            file_list = glob.glob(os.path.join(
                self.get_config_path(), configs['file']))
            for file in file_list:
                remote_filename = os.path.join(
                    configs['to'], os.path.basename(file))
                self.node.sftp_put(file, remote_filename)

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
        topic_info = systems["broker"]["topics"]
        manager = self._node_manager.get_manager()
        self._natsbox = NatsBox('nats-box', topic_info['server'])
        self._natsbox.node_name = manager.name
        self._natsbox.node = manager
        self._natsbox.networks = self._broker[0].networks
        self._natsbox.home_dir = self._topic_dir
        self._natsbox.set_stream_configs(topic_info['stream'])

    def deploy_broker(self):
        super().deploy_broker()
        print('------------Deploy nats-box containers------------')
        self._executre.up_containers(
            [self._natsbox], self._node_manager, self._natsbox.SERVICE)

    def remove_broker(self):
        super().remove_broker()
        print('------------Remove nats-box containers------------')
        self._executre.down_containers(
            [self._natsbox], self._node_manager, self._natsbox.SERVICE)

    def create_topics(self):
        command = self._natsbox.build_command_to_create_stream()
        self._executre.exec_command_in_container(self._natsbox, command)

    def describe_topics(self):
        command = self._natsbox.build_command_to_describe_stream()
        self._executre.exec_command_in_container(self._natsbox, command)
