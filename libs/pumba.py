from typing import List, Dict

from libs import utils
from libs.base.containers import Container


class TcContainer(Container):
    def __init__(self):
        super().__init__(name="tc-image", image="gaiadocker/iproute2")
        
    def pre_up_process(self):
        # 起動前に実行する処理なし
        pass

    def collect_results(self):
        # 収集する処理なし
        pass


class PumbaContainer(Container):
    def to_swarm(self):
        swarm = super().to_swarm()
        restart_policy = {
            'condition': 'on-failure'
        }
        swarm[self.name]['deploy'].update({'restart_policy': restart_policy})
        return swarm

    def pre_up_process(self):
        # 起動前に実行する処理なし
        pass

    def collect_results(self):
        # 収集する処理なし
        pass


class PumbaNetemContainer(PumbaContainer):
    def __init__(self, name: str, configs: dict, target_container: str):
        configs['image'] = 'gaiaadm/pumba'
        configs['volumes'] = [
            {
                'type': 'bind',
                'source': '/var/run/docker.sock',
                'target': '/var/run/docker.sock'
            }
        ]
        self.target_container = target_container
        self._mode = configs.pop('mode')
        self._start = configs.pop('start')
        self._duration = configs.pop('duration')
        self._destination = configs.pop('destination', [])
        self._params = configs.pop('params', {})
        super().__init__(name, **configs)

    @property
    def start(self):
        return utils.change_time_to_sec(self._start)

    def create_commnad(self, containers: List[Container]):
        cmd = f'--json --log-level info netem -d {self._duration} --tc-image gaiadocker/iproute2'
        for dest in self._destination:
            is_found = False
            for container in containers:
                if container.name == dest:
                    cmd += f' --target {container.internal_ip}'
                    is_found = True
            if not is_found:
                raise RuntimeError(f"Container {dest} is not found")
        cmd += f' {self._mode}'
        for param, value in self._params.items():
            cmd += f' --{param} {value}'
        cmd += f' re2:.*{self.target_container}.*'
        self.command = cmd
