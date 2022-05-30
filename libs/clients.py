import glob
import os

from libs.base.containers import Container


class ClientContainer(Container):
    def __init__(self, name, configs: dict):
        configs['image'] = 'todoroki182814/measurement-client'
        configs['volumes'] = [
            {
                'type': 'bind',
                'source': f'/tmp/{name}/configs',
                'target': '/clients/configs',
            },
            {
                'type': 'bind',
                'source': f'/tmp/{name}/results',
                'target': '/clients/results',
            }
        ]
        if 'networks' not in configs.keys():
            raise RuntimeError("Please specify the client network.")
        self._configs = [
            {
                'file': '*',
                'to': f'/tmp/{name}/configs'
            }
        ]
        self._results = [
            {
                'from': f'/tmp/{name}/results'
            }
        ]
        super().__init__(name, **configs)

    def pre_up_process(self):
        for configs in self._configs:
            file_list = glob.glob(os.path.join(
                self.get_config_path(), configs['file']))
            for file in file_list:
                remote_filename = os.path.join(
                    configs['to'], os.path.basename(file))
                self.node.sftp_put(file, remote_filename)

    def collect_results(self):
        for results in self._results:
            ls_res, _ = self.node.ssh_exec(f'ls {results["from"]}')
            for file_path in ls_res:
                filename = os.path.basename(file_path)
                self.node.sftp_get(os.path.join(results["from"], file_path), os.path.join(
                    self.get_result_path(), filename))

    def to_swarm(self):
        # 終了後に再起動しないようにエラー時のみ再起動するよう設定
        swarm = super().to_swarm()
        restart_policy = {
            'condition': 'none'
        }
        swarm[self.name]['deploy'].update({'restart_policy': restart_policy})
        return swarm


class CustemClientContainer(Container):
    def __init__(self, name, configs: dict):
        super().__init__(name, **configs)
