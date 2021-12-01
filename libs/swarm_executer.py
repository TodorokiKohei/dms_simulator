import os
from libs.base.executers import Executer

import yaml


class SwarmExecuter(Executer):

    def __init__(self, home_dir, remote_dir):
        super().__init__(home_dir, remote_dir)

        self._compose_dir = os.path.join(self._home_dir, 'compose-files')
        os.makedirs(self._compose_dir, exist_ok=True)

    def create_remote_dir(self, containers, node_manager):
        # マネージャーノードにコンテナ展開用のディレクトリを作成
        manager = node_manager.get_manager()
        manager.ssh_exec(f'mkdir -p {self._remote_dir}')

    def create_cluster(self, containers, node_manager):
        manager = node_manager.get_manager()

        # swarmの初期化
        res, err = manager.ssh_exec('docker swarm init')
        join_word = res[4].strip()

        # workerをクラスターに追加
        for worker in node_manager.get_workers():
            worker.ssh_exec(join_word)

        # ネットワークの作成
        networks = set()
        for container in containers:
            if container.networks is None:
                continue
            for network in container.networks:
                networks.add(network)
        for network in networks:
            manager.ssh_exec(f'docker network create -d overlay {network}')

    def delete_cluster(self, containers, node_manager):
        manager = node_manager.get_manager()

        # workerをクラスターから除外
        for worker in node_manager.get_workers():
            worker.ssh_exec('docker swarm leave')

        # swarmの削除
        manager.ssh_exec('docker swarm leave -f')

        # ネットワークの削除
        networks = set()
        for container in containers:
            if container.networks is None:
                continue
            for network in container.networks:
                networks.add(network)
        for network in networks:
            manager.ssh_exec(f'docker network rm {network}')

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
            self._compose_dir, f'docker-compose-{service}.yml')
        with open(compose_file, mode='w', encoding='utf-8') as f:
            yaml.safe_dump(compose, f, sort_keys=False)

        # managerノードにcomposeファイルを転送し、コンテナを展開する
        manager = node_manager.get_manager()
        remote_compose_file = os.path.join(
            self._remote_dir, f'docker-compose-{service}.yml')
        manager.scp_put(compose_file, remote_compose_file)
        manager.ssh_exec(
            f'docker stack deploy -c {remote_compose_file} {service}')

    def down_containers(self, containers, node_manager, service):
        # managerノードで展開したコンテナを削除する
        manager = node_manager.get_manager()
        manager.ssh_exec(f'docker stack rm {service}')
