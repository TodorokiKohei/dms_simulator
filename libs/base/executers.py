import os
from abc import ABCMeta, abstractclassmethod


class AbstractExecuter(metaclass=ABCMeta):

    @abstractclassmethod
    def create_remote_dir(self, containers, node_manager):
        pass

    @abstractclassmethod
    def create_cluster(self, containers, node_manager):
        pass

    @abstractclassmethod
    def delete_cluster(self, containers, node_manager):
        pass

    @abstractclassmethod
    def up_containers(self, containers, node_manager, service):
        pass

    @abstractclassmethod
    def down_containers(self, containers, node_manager, service):
        pass


class Executer(AbstractExecuter):

    def __init__(self, home_dir, remote_dir):
        self._home_dir = home_dir
        self._remote_dir = remote_dir

        os.makedirs(self._home_dir, exist_ok=True)