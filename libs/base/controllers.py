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
        