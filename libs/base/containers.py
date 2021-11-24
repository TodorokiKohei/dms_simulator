from abc import ABCMeta, abstractclassmethod


class AbstractContainer(metaclass=ABCMeta):
    @abstractclassmethod
    def to_swarm(self):
        pass


class Container(AbstractContainer):
    def __init__(self, name, node_name=None, ports=None, volumes=None, environment=None, networks=None,
                 command=None):
        self.image = None
        self.name = name
        self.node_name = node_name
        self.ports = ports
        self.volumes = volumes
        self.environemnt = environment
        self.networks = networks
        self.command = command

        self.node_hostname = None

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
        if self.command is not None:
            swarm['command'] = self.command
        swarm['deploy'] = {
            'mode': 'global',
            'placement':
                {'constraints': [f'node.hostname=={self.node_hostname}']}
        }
        return {self.name: swarm}
