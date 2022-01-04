import os
from typing import List

import yaml

from libs.base.containers import Container
from libs.base.executers import Executer
from libs.utils import NodeManager


class SwarmExecuter(Executer):

    def __init__(self, home_dir, remote_dir, debug_mode=False):
        super().__init__(home_dir, remote_dir)
        self._compose_dir = os.path.join(self._home_dir, 'compose-files')
        os.makedirs(self._compose_dir, exist_ok=True)
        self._debug_mode = debug_mode

    def _get_container_network(self, containers):
        """

        """
        networks = set()
        for container in containers:
            if container.networks is None:
                continue
            for network in container.networks:
                networks.add(network)
        return networks

    def create_remote_dir(self, containers: list, node_manager: NodeManager):
        manager = node_manager.get_manager()
        manager.ssh_exec(f'mkdir -p {self._remote_dir}')

    def create_cluster(self, containers: list, node_manager: NodeManager):
        manager = node_manager.get_manager()

        # swarmの初期化
        res, err = manager.ssh_exec('docker swarm init')
        join_word = res[4]

        # workerをクラスターに追加
        for worker in node_manager.get_workers():
            worker.ssh_exec(join_word)

        # ネットワークの作成
        networks = self._get_container_network(containers)
        for network in networks:
            manager.ssh_exec(f'docker network create -d overlay {network}')

    def delete_cluster(self, containers: list, node_manager: NodeManager):
        manager = node_manager.get_manager()

        # workerをクラスターから除外
        for worker in node_manager.get_workers():
            worker.ssh_exec('docker swarm leave')

        # swarmの削除
        manager.ssh_exec('docker swarm leave -f')

    def up_containers(self, containers: list, node_manager: NodeManager, service: str):
        # swarmで展開するためのcompose-fileの内容を作成
        swarm = {}
        for container in containers:
            container.pre_up_process()
            swarm.update(container.to_swarm())
        compose = {
            'version': '3.8',
            'services': swarm,
        }

        # コンテナに設定されているnetworkをcompose-fileの内容に追加
        networks = self._get_container_network(containers)
        if networks != set():
            compose['networks'] = {}
            for network in networks:
                compose['networks'][network] = {'external': True}

        # compose-fileを作成
        compose_file = os.path.join(
            self._compose_dir, f'docker-compose-{service}.yml')
        with open(compose_file, mode='w', encoding='utf-8') as f:
            yaml.safe_dump(compose, f, sort_keys=False)

        # managerノードにcompose-fileを転送し、コンテナを展開する
        manager = node_manager.get_manager()
        remote_compose_file = os.path.join(
            self._remote_dir, f'docker-compose-{service}.yml')
        manager.sftp_put(compose_file, remote_compose_file)
        if self._debug_mode:
            return
        manager.ssh_exec(
            f'docker stack deploy -c {remote_compose_file} {service}')

    def down_containers(self, containers: list, node_manager: NodeManager, service: str):
        # managerノードで展開したコンテナを削除する
        manager = node_manager.get_manager()
        manager.ssh_exec(f'docker stack rm {service}')

    def check_running(self, containers: list, node_manager: NodeManager, service: str):
        # Dockerのコマンドで展開しているコンテナを取得
        manager = node_manager.get_manager()
        serivce_info_all, _ = manager.ssh_exec(
            'docker service ls --format "{{.Name}} {{.Replicas}}"')
        service_name_list = []
        service_replica_list = []
        for service_info in serivce_info_all:
            service_info = service_info.strip()
            service_name, service_replica = service_info.split(' ')
            service_name_list.append(service_name)
            service_replica_list.append(service_replica)
        # 起動確認したいコンテナ名を探索し、Replica数と起動数が一致していれば正常起動としてTrueを設定
        results = {}
        for container in containers:
            service_name = service + "_" + container.name
            if service_name in service_name_list:
                index = service_name_list.index(service_name)
                service_replica = service_replica_list[index]
                real, ideal = service_replica.split('/')
                if ideal == '0':
                    results[container.name] = False
                elif real != ideal:
                    results[container.name] = False
                else:
                    results[container.name] = True
            else:
                results[container.name] = False
        return results

    def collect_logs(self, containers: list, node_manager: NodeManager, service: str):
        manager = node_manager.get_manager()
        for container in containers:
            # スタック名とコンテナ名からサービス名を作成
            service_name = f'{service}_{container.name}'
            _, log = manager.ssh_exec(f'docker service logs {service_name}')
            with open(os.path.join(container.home_dir, container.name)+'.log', mode='w') as f:
                f.writelines('\n'.join(log))

    def get_container_internal_ip(self, containers: List[Container], node_manager: NodeManager):
        nodes = set()
        for container in containers:
            nodes.add(container.node)

        internal_ip_list = []
        for node in nodes:
            names, _ = node.ssh_exec('docker ps --format "{{.Names}}"')
            for name in names:
                ip, _ = node.ssh_exec('docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" ' + name)
                internal_ip_list.append({'name':name.split('.')[0], 'ip':ip[0]})
        return internal_ip_list
