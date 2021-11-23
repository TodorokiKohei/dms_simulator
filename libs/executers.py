import os
from abc import ABCMeta, abstractclassmethod

import yaml


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
            node.ssh_exec_sudo(f'sudo rm -rf {vol["source"]}')

    @abstractclassmethod
    def up_containers(self, containers, node_manager, yaml_file):
        pass

    @abstractclassmethod
    def down_containers(self, containers, node_manager, yaml_file):
        pass


class SwarmExecuter(Executer):

    def __init__(self, home_dir, remote_dir):
        super().__init__(home_dir, remote_dir)
        self._compose_dir = os.path.join(self._home_dir, 'compose-files')

    def init(self, containers, node_manager):
        super().init(containers, node_manager)

        # swarmの初期化
        # worker追加


    def clean(self, containers, node_manager):
        super().clean(containers, node_manager)

        # ノードの除外
        # swarmの削除

    def up_containers(self, containers, node_manager, service):
        # swarmで展開するためのcomposeファイルを作成
        swarm = {}
        for container in containers:
            swarm.update(container.to_swarm())
        compose = {
            'version': '3.8',
            'services': swarm,
            'networks': {
                'kafka-network': {
                    'external': True
                }
            }
        }
        compose_file = os.path.join(
            self._compose_dir, 'docker-compose-{service}.yml')
        with open(compose_file, mode='w', encoding='utf-8') as f:
            yaml.safe_dump(compose, f, sort_keys=False)

        # managerノードにcomposeファイルを転送し、コンテナを展開する
        manager = node_manager.get_manager()
        remote_compose_file = os.path.join(
            self._remote_dir, 'docker-compose-{service}.yml')
        manager.scp_put(compose_file, remote_compose_file)
        stdin, stdout, stderr = manager.ssh_exec(
            f'docker stack deploy -c {remote_compose_file} {service}')
        print(stdout.readlines())
        print(stderr.readlines())

    def down_containers(self, containers, node_manager, service):
        # managerノードで展開したコンテナを削除する
        manager = node_manager.get_manager()
        stdin, stdout, stderr = manager.ssh_exec(f'docker stack rm {service}')
        print(stdout.readlines())
        print(stderr.readlines())
