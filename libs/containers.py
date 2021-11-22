from abc import ABCMeta, abstractclassmethod

from yaml.events import NodeEvent


class Container(metaclass=ABCMeta):
    def __init__(self, name, node, ports=None, volumes=None, environment=None, networks=None,
        args=None, command=None):
        self.image = None
        self.name = name
        self.node = node
        self.ports = ports
        self.volumes = volumes
        self.environemnt = environment
        self.networks = networks
        self.args = args
        self.command= command

    def to_swarm(self):
        swarm = {
            'image': self.image,
            'hostname': self.name,
        }
        if self.ports is not None:
            swarm['ports'] = self.ports
        if self.volumes is not None:
            swarm['volumes'] = self.volumes
        if self.environemnt is not None:
            swarm['environment'] = self.environemnt
        if self.networks is not None:
            swarm['networks'] = self.networks
        if self.args is not None:
            swarm['args'] = self.args
        if self.command is not None:
            swarm['command'] = self.command
        swarm['deploy'] = {
            'mode': 'global',
            'placement':
                {'constraints': [f'node.hostname=={self.node}']}
            }
        return {self.name: swarm}


class ZookeeperContainer(Container):
    def __init__(self, name, configs):
        super().__init__(name, **configs)
        self.image = "confluentinc/cp-zookeeper:5.5.6"
        if self.volumes is None:
            self.volumes = [
                {
                    'type':'bind',
                    'source': f'/tmp/{self.name}/log',
                    'target': '/var/lib/zookeeper/log'
                },
                {
                    'type':'bind',
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
                    'type':'bind',
                    'source': f'/tmp/{self.name}/data',
                    'target': '/var/lib/kafka/data'
                },
            ]
        if self.networks is None:
            self.networks = ['kafka-network']