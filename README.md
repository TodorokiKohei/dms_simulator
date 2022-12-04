# dms_simulator
分散メッセージングシステムの障害検証を行うためのツール。

# 実行環境
## OS
Ubuntu 20.04

## 起動方法
### ツールを実行するサーバでの設定
* sshの秘密鍵，公開鍵の作成
    ```
    ssh-keygen
    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/todoroki/.ssh/id_rsa):
    Enter passphrase (empty for no passphrase):
    Enter same passphrase again:
    ```
* Pythonの実行環境
    ```
    sudo apt install python3.8 python3.8-venv -y
    python3.8 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

### メッセージングシステムを展開するサーバでの設定
* ツールを実行するサーバからSSHでアクセスできるように設定
  * SSHの鍵を配信
    ```
    scp .ssh/id_rsa.pub todoroki@<IP>:~
    ```
  * SSHの鍵を設定
    ```
    mkdir .ssh
    cat id_rsa.pub >> .ssh/authorized_keys
    chmod 700 .ssh
    chmod 600 .ssh/authorized_keys
    ```
* Dockerのインストール
    ```
    sudo apt-get remove docker docker-engine docker.io containerd runc
    sudo apt-get update
    sudo apt-get install -y \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg \
        lsb-release
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo \
    "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io
    sudo gpasswd -a $(whoami) docker

    sudo curl -L https://github.com/docker/compose/releases/download/1.29.2/docker-compose-Linux-x86_64 -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    curl -L https://raw.githubusercontent.com/docker/compose/$(docker-compose version --short)/contrib/completion/bash/docker-compose | sudo tee /etc/bash_completion.d/docker-compose > /dev/null
    ```
* template.yamlの作成  
`exmaples`を参照

* 設定ファイルを配置するディレクトリを作成する
    ```
    python dms_simulater.py -m create_dir -f ./examples/template_nats.yml

    exec_dir
    | - controller
          | - nats
                | - nats-1
                |   | - configs
                |   | - results
                | - nats-pub-1
                |   | - configs
                |   | - results
                | - nats-sub-1
                |   | - configs
                |   | - results            
                | - topic_configs
    ```

* クライアントの設定ファイルを配置する  
`examples/clients_configs`を参照。クライアントは[measurement_client](https://github.com/TodorokiKohei/measurement_client)を利用する。
    ```
    exec_dir
     | - controller
          | - nats
                | - nats-1
                |   | - configs
                |   |     | - natsconf.yaml
                |   | - results
                | - nats-pub-1
                |   | - configs
                |   |     | - natsconf.yaml
                |   | - results
                | - nats-sub-1
                |   | - configs
                |   | - results            
                | - topic_configs
    ```

* シミュレーションを実行する
    ```
    source venv/bin/activate
    python dms-simulater -m run
    ```