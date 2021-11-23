import os
from abc import ABCMeta, abstractclassmethod


class AbstractExecuter(metaclass=ABCMeta):
    @abstractclassmethod
    def init(self, containers, node_manager):
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


class Executer(metaclass=ABCMeta):

    def __init__(self, home_dir, remote_dir):
        self._home_dir = home_dir
        self._remote_dir = remote_dir

    def init(self, containers, node_manager):
        # ディレクトリ作成
        os.makedirs(self._home_dir, exist_ok=True)
        manager = node_manager.get_manager()
        manager.ssh_exec(f'mkdir -p {self._remote_dir}')

        os.makedirs(self._compose_dir, exist_ok=True)
        for container in containers:
            self._create_volume_dir(container, node_manager)

    def _create_volume_dir(self, container, node_manager):
        # コンテナのボリュームに設定されているパスを作成
        if container.volumes is None:
            return
        node = node_manager.get_match_node(container.node)
        for vol in container.volumes:
            print(f'[{node.name}]: mkdir -p {vol["source"]}')
            node.ssh_exec(f'mkdir -p {vol["source"]}')

    def clean(self, containers, node_manager):
        for container in containers:
            self._del_volume_dir(container, node_manager)

    def _del_volume_dir(self, container, node_manager):
        # コンテナのボリュームに設定されているパスを削除
        if container.volumes is None:
            return
        node = node_manager.get_match_node(container.node)
        for vol in container.volumes:
            print(f'[{node.name}]: sudo rm -rf {vol["source"]}')
            node.ssh_exec_sudo(f'sudo rm -rf {vol["source"]}')
