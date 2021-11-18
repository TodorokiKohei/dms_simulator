from abc import ABCMeta, abstractclassmethod


class Container(metaclass=ABCMeta):
    def __init__(self, name, node, ports=None, volumes=None, environment=None, networks=None):
        self.name = name
        self.node = node
        self.ports = ports
        self.volumes = volumes
        self.environemnt = environment
        self.networks = networks


    def to_swarm(self):
        swarm = {
            self.name: {
                'hostname': self.name,
                'ports': self.ports,
                'volumes': self.volumes,
                'environment': self.environemnt,
                'networks': self.networks,
                'deploy':{
                    'mode': 'global',
                    'placement':
                        {'constraints': f'node.labels.type=={self.node}'}
                    }
            }
        }
        return swarm


class ZookeeperContainer(Container):
    def __init__(self, name, node, configs):
        super().__init__(name, node, **configs)
        self.image = "confluentinc/cp-zookeeper:5.5.6"
        if self.volumes is None:
            self.volumes = [
                f'/tmp/{self.name}/log:/var/lib/zookeeper/log',
                f'/tmp/{self.name}/data:/var/lib/zookeeper/data',
            ]

    def to_swarm(self):
        swarm = super().to_swarm()
        swarm['image'] = self.image
        return swarm


class KafkaContainer(Container):
    def __init__(self, name, node, configs):
        super().__init__(name, node, **configs)
        self.image = "confluentinc/cp-kafka:5.5.6"
        if self.volumes is None:
            self.volumes = [
                f'/tmp/{self.name}/data:/var/lib/kafka/data',
            ]

    def to_swarm(self):
        swarm = super().to_swarm()
        swarm['image'] = self.image
        return swarm
