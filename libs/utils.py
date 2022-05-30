import re
import time
import threading
import pprint

import paramiko

lock = threading.Lock()


def connect(func):
    """
    ssh接続を確立してからコマンドを実行する関数
    """

    def exec(self, *args, **kwargs):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.address, username=self.user,
                       key_filename=self.keyfile, timeout=5)
        kwargs['client'] = client
        res, err = func(self, *args, **kwargs)
        client.close()
        res = [s.rstrip() for s in res]
        err = [s.rstrip() for s in err]
        with lock:
            if res != []:
                pprint.pprint(res, width=160)
            if err != []:
                print("************** ERROR **************")
                pprint.pprint(err, width=160)
        return (res, err)
    return exec


class Node:
    def __init__(self, name, user, keyfile, sudopass, address, is_manager=False):
        self.name = name
        self.user = user
        self.keyfile = keyfile
        self.sudopass = sudopass
        self.address = address
        self.is_manager = is_manager
        self._hostname = None

    @property
    def hostname(self):
        if self._hostname is not None:
            return self._hostname
        res, _ = self.ssh_exec('hostname')
        self._hostname = res[0].replace('\n', '')
        return self._hostname

    @connect
    def ssh_exec(self, cmd, client):
        print(f'[{self.name}]: {cmd}')
        stdin, stdout, stderr = client.exec_command(cmd)
        stdout.channel.recv_exit_status()
        return (stdout.readlines(), stderr.readlines())

    @connect
    def ssh_exec_sudo(self, cmd, client):
        print(f'[{self.name}]: {cmd}')
        stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)
        cnt = 0
        while len(stdout.channel.in_buffer) == 0:
            time.sleep(0.1)
            cnt += 0.1
            if cnt == 5:
                raise RuntimeError(f"Failur {cmd}")
        stdin.channel.send(self.sudopass + "\n")
        stdin.flush()
        stdout.channel.recv_exit_status()
        return (stdout.readlines(), stderr.readlines())

    @connect
    def sftp_put(self, local, remote, client):
        print(f'[{self.name}]: sftp {local} {remote}')
        with client.open_sftp() as sftp:
            sftp.put(local, remote)
        return ("", "")

    @connect
    def sftp_get(self, remote, local, client):
        print(f'[{self.name}]: sftp {remote} {local}')
        with client.open_sftp() as sftp:
            sftp.get(remote, local)
        return ("", "")

    def match_name(self, name):
        return self.name == name


class NodeManager:
    def __init__(self):
        self._nodes = []

    def append(self, node):
        self._nodes.append(node)

    def get_manager(self) -> Node:
        for node in self._nodes:
            if node.is_manager:
                return node

    def get_workers(self):
        workers = []
        for node in self._nodes:
            if not node.is_manager:
                workers.append(node)
        return workers

    def get_nodes(self):
        return self._nodes

    def get_match_node(self, node_name):
        for node in self._nodes:
            if node.match_name(node_name):
                return node


def change_time_to_sec(t):
    """
    h,m,s指定の時間を秒(数値)に変換
    """
    if re.search('h|m|s', t) is None:
        raise ValueError(
            'Please specify "s" or "m" or "h" for the duration')
    sec = 0
    for sep in ['h', 'm', 's']:
        if sep in t:
            val, t = t.split(sep)
            if sep == 'h':
                sec += int(val) * 60 * 60
            elif sep == 'm':
                sec += int(val) * 60
            elif sep == 's':
                sec += int(val)
    return sec


def change_size_to_byte(size):
    """
    m,k指定のサイズをバイト(数値)に変換
    """
    byte = 0
    if 'm' in size:
        byte = int(size.split('m')[0]) * 1000 * 1000
    elif 'k' in size:
        byte = int(size.split('k')[0]) * 1000
    else:
        byte = int(size)
    return byte
