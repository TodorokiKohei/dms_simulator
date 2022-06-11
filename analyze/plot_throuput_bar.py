
import math
import os
from glob import glob

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = "Meiryo"


def get_broker_info_from_dir(file_path):
    dir_name = os.path.basename(os.path.dirname(file_path))
    dir_info = dir_name.split('_')
    broker = dir_info[2]
    message_size = dir_info[5]
    if (len(dir_info) > 6):
        broker = f'{broker}-{"-".join(dir_info[6:])}'
    return (broker, message_size)


def collect_throughput(root_dir, pattern):
    file_list = glob(os.path.join(root_dir, pattern))
    file_list.sort()
    result = {}
    for file_path in file_list:
        broker, message_size = get_broker_info_from_dir(file_path)
        throughput = np.loadtxt(
            file_path, delimiter=',', skiprows=1, dtype=np.int64)
        if broker not in result.keys():
            result[broker] = {}
        result[broker][message_size] = throughput
    return result


def collect_latency(root_dir, pattern):
    file_list = glob(os.path.join(root_dir, pattern))
    file_list.sort()
    message_df_list = {}
    fig, ax = plt.subplots()
    for file_path in file_list:
        broker, message_size = get_broker_info_from_dir(file_path)
        message_df = pd.read_csv(file_path)

        if broker not in message_df_list.keys():
            message_df_list[broker] = {}
        message_df_list[broker][message_size] = message_df
    return message_df_list

    # ax.plot(message_df.loc[:,'seqNum'],
    #  message_df.loc[:,'receivedTime']-message_df.loc[:,'sentTime'])
    # fig.savefig(os.path.join(root_dir, f'{broker}_{message_size}_latency.png'))
    # ax.cla()


def plot_bar_for_each(root_dir, throughput_map, mode):
    fig, ax = plt.subplots()
    for broker, msg_size_map in throughput_map.items():
        x = np.arange(len(msg_size_map))
        mean_msg = []
        std_msg = []
        mean_byte = []
        std_byte = []
        x_ticklabels = []
        for sz, throughput in msg_size_map.items():
            mean_msg.append(throughput[2:, 1].mean())
            std_msg.append(throughput[2:, 1].std())
            mean_byte.append((throughput[2:, 2]/1000/1000).mean())
            std_byte.append((throughput[2:, 2]/1000/1000).std())
            x_ticklabels.append(sz)
        plt.bar(x, np.array(mean_msg), yerr=np.array(std_msg), capsize=5)
        ax.set_title(f"{broker}")
        ax.set_ylabel("Throughput[msg/s]")
        ax.set_xlabel("Message Size")
        ax.set_xticks(x)
        ax.set_xticklabels(x_ticklabels)
        fig.savefig(os.path.join(root_dir, f'{mode}_{broker}_msg.png'))
        ax.cla()

        plt.bar(x, np.array(mean_byte), yerr=np.array(std_byte), capsize=5)
        ax.set_title(f"{broker}")
        ax.set_ylabel("Throughput[MB/s]")
        ax.set_xlabel("Message Size")
        ax.set_xticks(x)
        ax.set_xticklabels(x_ticklabels)
        fig.savefig(os.path.join(root_dir, f'{mode}_{broker}_byte.png'))
        ax.cla()


def plot_bar_from_key_latency(root_dir, latency_map, mode, match_keys, **kwargs):
    fig, ax = plt.subplots()
    if match_keys == []:
        match_keys = latency_map.keys()
    width = 0.4
    start_i = 2
    x_start = 0
    x = []
    x_ticks = []
    x_ticklabels = []
    labels = []
    mean_latency = []
    std_latency = []
    is_first = True
    for key in match_keys:
        msg_size_map = latency_map[key]
        # 棒グラフのサイズ分だけずらしたxの値を作成
        x_range = [x_start + i*width for i in range(len(msg_size_map))]
        x.extend(x_range)
        x_start += math.ceil(width * len(msg_size_map)) + 1
        x_ticks.append((x_range[-1] + x_range[0])/2)
        x_ticklabels.append(key)
        for sz, latency in msg_size_map.items():
            mean_latency.append(latency[start_i:].mean())
            std_latency.append(latency[start_i:].std())
            if (is_first):
                labels.append(sz)
        is_first = False

    sz_num = len(labels)
    for i, label in enumerate(labels):
        # ax.bar(x[i::sz_num], np.array(mean_latency[i::sz_num]),
        #        yerr=np.array(std_latency[i::sz_num]), width=width, capsize=2, label=label)
        ax.bar(x[i::sz_num], np.array(mean_latency[i::sz_num]),
               width=width, capsize=2, label=label)

    if ('title' in kwargs.keys()):
        ax.set_title(kwargs['title'])
    else:
        ax.set_title("_".join(match_keys))

    if ('xlabel' in kwargs.keys()):
        ax.set_xlabel(kwargs['xlabel'])

    if ('ylabel' in kwargs.keys()):
        ax.set_ylabel(kwargs['ylabel'])
    else:
        ax.set_ylabel("latency[ms]")

    ax.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax.set_xticklabels(x_ticklabels)

    if ('yscale' in kwargs.keys()):
        ax.set_yscale(kwargs['yscale'])

    ax.legend()
    fig.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_msg.png'), bbox_inches='tight')


def plot_bar_from_key(root_dir, throughput_map, mode, match_keys, res_lower_limit, **kwargs):
    fig_msg, ax_msg = plt.subplots()
    fig_byte, ax_byte = plt.subplots()
    if match_keys == []:
        match_keys = throughput_map.keys()
    width = 0.4
    start_i = 4
    x_start = 0
    x = []
    x_ticks = []
    x_ticklabels = []
    labels = []
    mean_msg = []
    std_msg = []
    mean_byte = []
    std_byte = []
    is_first = True
    for broker in match_keys:
        msg_size_map = throughput_map[broker]
        # 棒グラフのサイズ分だけずらしたxの値を作成
        x_range = [x_start + i*width for i in range(len(msg_size_map))]
        x.extend(x_range)
        x_start += math.ceil(width * len(msg_size_map)) + 1
        x_ticks.append((x_range[-1] + x_range[0])/2)
        x_ticklabels.append(broker)
        for sz, throughput in msg_size_map.items():
            mean_msg.append(throughput[start_i:-1, 1].mean())
            std_msg.append(throughput[start_i:-1, 1].std())
            mean_byte.append((throughput[start_i:-1, 2]/1000/1000).mean())
            std_byte.append((throughput[start_i:-1, 2]/1000/1000).std())
            if (is_first):
                labels.append(sz)
        is_first = False

    sz_num = len(labels)
    for i, label in enumerate(labels):
        # ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
        #            yerr=np.array(std_msg[i::sz_num]), width=width, capsize=2, label=label)
        # ax_byte.bar(x[i::sz_num], np.array(mean_byte[i::sz_num]),
        #             yerr=np.array(std_byte[i::sz_num]), width=width, capsize=2, label=label)
        ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
                   width=width, capsize=2, label=label)
        # ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
        #            yerr=np.array([min_msg[i::sz_num], max_msg[i::sz_num]]), width=width, capsize=2, label=label)
        ax_byte.bar(x[i::sz_num], np.array(mean_byte[i::sz_num]),
                    width=width, capsize=2, label=label)

    if ('title' in kwargs.keys()):
        ax_msg.set_title(kwargs['title'])
    else:
        ax_msg.set_title("_".join(match_keys))

    if ('xlabel' in kwargs.keys()):
        ax_msg.set_xlabel(kwargs['xlabel'])

    if ('ylabel' in kwargs.keys()):
        ax_msg.set_ylabel(kwargs['ylabel'])
    else:
        ax_msg.set_ylabel("Throughput[msg/s]")

    ax_msg.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax_msg.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax_msg.set_xticklabels(x_ticklabels)

    if ('msg_yscale' in kwargs.keys()):
        ax_msg.set_yscale(kwargs['msg_yscale'])

    ax_msg.legend()
    fig_msg.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_msg.png'), bbox_inches='tight')

    if ('title' in kwargs.keys()):
        ax_byte.set_title(kwargs['title'])
    else:
        ax_byte.set_title("_".join(match_keys))

    if ('xlabel' in kwargs.keys()):
        ax_byte.set_xlabel(kwargs['xlabel'])

    if ('ylabel' in kwargs.keys()):
        ax_byte.set_ylabel(kwargs['ylabel'])
    else:
        ax_byte.set_ylabel("Throughput[MB/s]")

    ax_byte.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax_byte.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax_byte.set_xticklabels(x_ticklabels)

    if ('byte_yscale' in kwargs.keys()):
        ax_byte.set_yscale(kwargs['byte_yscale'])
    ax_byte.legend()
    fig_byte.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_byte.png'), bbox_inches='tight')


def plot_single(res_lower_limit):
    """
    1台構成でのプロット
    """
    root_dir = 'results/2022_0610_2359_100b_64kb'
    # root_dir = 'results/2022_0611_0150_latency'

    # Publishserのスループット
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throuput*")
    pub_throughput_map.update(collect_throughput(
        root_dir, "**/*publisher*throughput*"))
    # plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", ['kafka-acks-0', 'kafka-acks-1', 'nats', 'jetstream'], res_lower_limit,
                      title="1台構成でのPublisherのスループット", msg_yscale='log', byte_yscale='log')

    # Subscriberのスループット
    sub_throughput_map = collect_throughput(
        root_dir, "**/*subscriber*throuput*")
    sub_throughput_map.update(collect_throughput(
        root_dir, "**/*subscriber*throughput*"))
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ['kafka-acks-0', 'kafka-acks-1', 'nats', 'jetstream'], res_lower_limit,
                      title="1台構成でのSubscriberのスループット", msg_yscale='log', byte_yscale='log')

    # PublisherとSubscriberのスループット比較
    for broker, pub_throughput in pub_throughput_map.items():
        throughput_map = {}
        throughput_map['pub_' + broker] = pub_throughput
        throughput_map['sub_' + broker] = sub_throughput_map[broker]
        plot_bar_from_key(root_dir, throughput_map, 'pub-sub',
                          ['pub_'+broker, 'sub_'+broker], res_lower_limit)

    # Broker毎のレイテンシー
    broker_list = ['kafka', 'jetstream', 'nats']
    latency_map = {}
    for broker in broker_list:
        if broker == 'kafka':
            df_list = collect_latency(
                root_dir, f"**/*{broker}-subscriber-?-?.csv")
        elif broker == 'jetstream':
            df_list = collect_latency(
                root_dir, f"**/*{broker}-subscriber-pull-?.csv")
        else:
            df_list = collect_latency(
                root_dir, f"**/*{broker}-subscriber-?.csv")
        for broker in df_list:
            latency = {}
            for msg_size, df in df_list[broker].items():
                latency[msg_size] = (
                    df.loc[:, 'receivedTime'] - df.loc[:, 'sentTime']).values
            latency_map[broker] = latency
    plot_bar_from_key_latency(root_dir, latency_map, "latency", ['kafka-acks-0', 'kafka-acks-1', 'nats', 'jetstream'],
                              title="1台構成でのレイテンシー", yscale='log')


def plot_single_kafka(res_lower_limit):
    """
    Kafkaの1台構成でのプロット
    """
    root_dir = 'results/2022_0604_kafka_buff_100b_16kb'
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throuput*")
    plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", [
                      "kafka-acks-0", "kafka-acks-1", "kafka-acks-0-buff-32k-linger-10", "kafka-acks-1-buff-32k-linger-10"],
                      res_lower_limit,
                      title="Kafka when changing parameters",
                      xtickslabels=["ack=0", "ack=1", "ack=0\nch params", "ack=1\nch params"])

    # sub_throughput_map = collect_throughput(root_dir, "**/*subscriber*throuput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", [
    #                   "kafka-acks-0", "kafka-acks-1", "kafka-acks-0-buff-32k-linger-10", "kafka-acks-1-buff-32k-linger-10"],
    #                   title="Kafka when changing parameters",
    #                   xtickslabels=["ack=0", "ack=1", "ack=0\nch params", "ack=1\nch params"])


def plot_cluster(res_lower_limit):
    """
    クラスター構成でのプロット
    """
    # root_dir = 'results/2022_0609_2256_cluster_100b_8kb'
    root_dir = 'results/2022_0611_0324_cluster_100b_64kb'

    # pub_throughput_map = collect_throughput(root_dir, "**/*publisher*throuput*")
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throughput*")
    # plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    # plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-acks-0-cl-3", "kafka-acks-1-cl-3", "jetstream-cl-3"])

    sub_throughput_map = collect_throughput(
        root_dir, "**/*subscriber*throughput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ["kafka-acks-0-cl-3", "kafka-acks-1-cl-3", "jetstream-cl-3"])

    for broker, _ in pub_throughput_map.items():
        broker_info = broker.split('-')
        if len(broker_info) != 1 and broker_info[-2] == 'cl':
            continue
        throughput_map = {}
        throughput_map['pub-'+broker] = pub_throughput_map[broker]
        throughput_map['pub-'+broker +
                       '-cl-3'] = pub_throughput_map[broker+'-cl-3']
        throughput_map['sub-'+broker] = sub_throughput_map[broker]
        throughput_map['sub-'+broker +
                       '-cl-3'] = pub_throughput_map[broker+'-cl-3']
        print(throughput_map.keys())
        plot_bar_from_key(root_dir, throughput_map, 'pub-sub', throughput_map.keys(), res_lower_limit,
                          title=broker+': 1台構成とレプリケーション構成', 
                          xtickslabels=['pub-single', 'pub-replication', 'sub-single', 'sub-replication'], 
                          msg_yscale='log', 
                          byte_yscale='log')

    for broker, _ in pub_throughput_map.items():
        broker_info = broker.split('-')
        if len(broker_info) != 1 and broker_info[-2] == 'cl':
            continue

        latency_map = {}
        if broker_info[0] == 'kafka' and broker_info[2] == '1':
            match_keys = ['kafka-acks-1', 'kafka-acks-1-cl-3']
            df_dict = collect_latency(
                root_dir, "**acks_1/*kafka-subscriber-?-?.csv")
            df_dict.update(collect_latency(
                root_dir, "**acks_1_cl_3/*kafka-subscriber-?-?.csv"))
        elif broker_info[0] == 'kafka' and broker_info[2] == 'all':
            match_keys = ['kafka-acks-all', 'kafka-acks-all-cl-3']
            df_dict = collect_latency(
                root_dir, "**acks_all/*kafka-subscriber-?-?.csv")
            df_dict.update(collect_latency(
                root_dir, "**acks_all_cl_3/*kafka-subscriber-?-?.csv"))
        elif broker_info[0] == 'jetstream':
            match_keys = ['jetstream', 'jetstream-cl-3']
            df_dict = collect_latency(
                root_dir, "**b/*jetstream-subscriber-pull-?.csv")
            df_dict.update(collect_latency(
                root_dir, "**cl_3/*jetstream-subscriber-pull-?.csv"))

        for key, msg_map in df_dict.items():
            latency = {}
            for msg_size, df in msg_map.items():
                latency[msg_size] = (
                    df.loc[:, 'receivedTime'] - df.loc[:, 'sentTime']).values
            latency_map[key] = latency

        plot_bar_from_key_latency(root_dir, latency_map, "latency", match_keys,
                                  title=broker+": レプリケーション構成でのレイテンシー", yscale='log', xtickslabels=['latency-single', 'latency-replication'])


def plot_tool_and_notool():
    """
    ツール有とツール無の比較用プロット
    """
    root_dir = 'results/2022_0609_0131_tool_and_notool'
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throughput*")
    pub_kafka_map = {}
    pub_kafka_map['kafka-100B'] = {
        'no tool': pub_throughput_map['kafka-acks-0-notool']['100b'],
        'tool': pub_throughput_map['kafka-acks-0']['100b']
    }
    pub_kafka_map['kafka-1KB'] = {
        'no tool': pub_throughput_map['kafka-acks-0-notool']['1kb'],
        'tool': pub_throughput_map['kafka-acks-0']['1kb']
    }
    pub_kafka_map['kafka-16KB'] = {
        'no tool': pub_throughput_map['kafka-acks-0-notool']['16kb'],
        'tool': pub_throughput_map['kafka-acks-0']['16kb']
    }
    pub_nats_map = {}
    pub_nats_map['nats-100B'] = {
        'no tool': pub_throughput_map['nats-notool']['100b'],
        'tool': pub_throughput_map['nats']['100b']
    }
    pub_nats_map['nats-1KB'] = {
        'no tool': pub_throughput_map['nats-notool']['1kb'],
        'tool': pub_throughput_map['nats']['1kb']
    }
    pub_nats_map['nats-16KB'] = {
        'on VM': pub_throughput_map['nats-notool']['16kb'],
        'on Tool': pub_throughput_map['nats']['16kb']
    }
    plot_bar_from_key(root_dir, pub_kafka_map, "pub", [], res_lower_limit)
    plot_bar_from_key(root_dir, pub_nats_map, "pub", [], res_lower_limit)

    kafka_df_list = collect_latency(root_dir, "**/*kafka-subscriber-?-?.csv")
    nats_df_list = collect_latency(root_dir, "**/*nats-subscriber-?.csv")
    kafka_latency_map = {}
    kafka_latency_map['kafka-100B'] = {
        'no tool': (kafka_df_list['kafka-acks-0-notool']['100b'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0-notool']['100b'].loc[:, 'sentTime']).values,
        'tool': (kafka_df_list['kafka-acks-0']['100b'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0']['100b'].loc[:, 'sentTime']).values
    }
    kafka_latency_map['kafka-1KB'] = {
        'no tool': (kafka_df_list['kafka-acks-0-notool']['1kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0-notool']['1kb'].loc[:, 'sentTime']).values,
        'tool': (kafka_df_list['kafka-acks-0']['1kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0']['1kb'].loc[:, 'sentTime']).values
    }
    kafka_latency_map['kafka-16KB'] = {
        'no tool': (kafka_df_list['kafka-acks-0-notool']['16kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0-notool']['16kb'].loc[:, 'sentTime']).values,
        'tool': (kafka_df_list['kafka-acks-0']['16kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0']['16kb'].loc[:, 'sentTime']).values
    }
    plot_bar_from_key_latency(root_dir, kafka_latency_map, "latency", [])

    nats_latency_amp = {}
    nats_latency_amp['nats-100B'] = {
        'no tool': (nats_df_list['nats-notool']['100b'].loc[:, 'receivedTime']-nats_df_list['nats-notool']['100b'].loc[:, 'sentTime']).values,
        'tool': (nats_df_list['nats']['100b'].loc[:, 'receivedTime']-nats_df_list['nats']['100b'].loc[:, 'sentTime']).values
    }
    nats_latency_amp['nats-1KB'] = {
        'no tool': (nats_df_list['nats-notool']['1kb'].loc[:, 'receivedTime']-nats_df_list['nats-notool']['1kb'].loc[:, 'sentTime']).values,
        'tool': (nats_df_list['nats']['1kb'].loc[:, 'receivedTime']-nats_df_list['nats']['1kb'].loc[:, 'sentTime']).values
    }
    nats_latency_amp['nats-16KB'] = {
        'no tool': (nats_df_list['nats-notool']['16kb'].loc[:, 'receivedTime']-nats_df_list['nats-notool']['16kb'].loc[:, 'sentTime']).values,
        'tool': (nats_df_list['nats']['16kb'].loc[:, 'receivedTime']-nats_df_list['nats']['16kb'].loc[:, 'sentTime']).values
    }
    plot_bar_from_key_latency(root_dir, nats_latency_amp, "latency", [])


if __name__ == '__main__':
    res_lower_limit = 100
    # plot_single(res_lower_limit)
    # plot_single_kafka()
    plot_cluster(res_lower_limit)
    # plot_tool_and_notool(res_lower_limit)
