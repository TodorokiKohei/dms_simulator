from abc import ABCMeta, abstractclassmethod


class AbstrctController(metaclass=ABCMeta):
    @abstractclassmethod
    def initialize(self):
        pass

    @abstractclassmethod
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


class Controller(metaclass=ABCMeta):
    def __init__(self, node_manager, executer):
        self._node_manager = node_manager
        self._executre = executer

        self._containers = []
        self._broker = []
        self._publisher = []
        self._subscriber = []

    def initialize(self):
        for container in self._containers:
            container.create_volume_dir()
        self._executre.create_remote_dir(self._containers, self._node_manager)
        self._executre.create_cluster(self._containers, self._node_manager)

    def clean(self):
        for container in self._containers:
            container.delete_volume_dir()
        self._executre.delete_cluster(self._containers, self._node_manager)
