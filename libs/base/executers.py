import os
from abc import ABCMeta, abstractclassmethod


class AbstractExecuter(metaclass=ABCMeta):
    @abstractclassmethod
    def initialize(self, containers, node_manager):
        pass

    @abstractclassmethod
    def clean(self, containers, node_manager):
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

    def initialize(self, containers, node_manager):
        os.makedirs(self._home_dir, exist_ok=True)

        # ボリューム作成
        for container in containers:
            self._create_volume_dir(container, node_manager.get_match_node(container.node_name))

    def _create_volume_dir(self, container, node):
        # コンテナのボリュームに設定されているパスを作成
        if container.volumes is None:
            return
        for vol in container.volumes:
            node.ssh_exec(f'mkdir -p {vol["source"]}')


    def clean(self, containers, node_manager):
        for container in containers:
            self._del_volume_dir(container, node_manager.get_match_node(container.node_name))

    def _del_volume_dir(self, container, node):
        if container.volumes is None:
            return
        for vol in container.volumes:
            node.ssh_exec_sudo(f'sudo rm -rf {vol["source"]}')