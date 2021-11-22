import os
from abc import ABCMeta, abstractclassmethod

import yaml

class Executer(metaclass=ABCMeta):

    def __init__(self, home_dir):
        self._home_dir = home_dir
        os.makedirs(self._home_dir, exist_ok=True)

    @abstractclassmethod
    def init(self, node_manager):
        pass

    @abstractclassmethod
    def clean(self):
        pass

    @abstractclassmethod
    def up_containers(self, containers, node_manager, yaml_file):
        pass

class SwarmExecuter(Executer):
    def init(self, node_manager):
        # swarmの初期化
        # worker追加
        pass

    def clean(self):
        # ノードの除外
        # swarmの削除
        pass

    def up_containers(self, containers, node_manager, yaml_file):
        # swarmで展開するためのcomposeファイルを作成
        swarm = {}
        for container in containers:
            swarm.update(container.to_swarm())
        compose = {
            'version': '3.8', 
            'services': swarm,
            'networks':{
                'kafka-network': {
                    'external': True
                    }
                }
            }
        compose_file = os.path.join(self._home_dir, yaml_file)
        with open(compose_file, mode='w', encoding='utf-8') as f:
            yaml.safe_dump(compose, f, sort_keys=False)

        # managerノードに転送し、展開する
        manager = node_manager.get_manager()
        remote_compose_file = os.path.join('/tmp', yaml_file)
        manager.scp_put(compose_file, remote_compose_file)
        manager.ssh_exec(f'docker stack deploy -c {remote_compose_file} kafka')
