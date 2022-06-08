

import argparse
import math
import os
from glob import glob

import numpy as np
import matplotlib.pyplot as plt


def collect_throughput(root_dir, pattern):
    pub_file_list = glob(os.path.join(root_dir, pattern))
    pub_file_list.sort()
    result = {}
    for pub_file_path in pub_file_list:
        dir_name = os.path.basename(os.path.dirname(pub_file_path))
        dir_info = dir_name.split('_')
        broker = dir_info[2]
        message_size = dir_info[5]
        if (len(dir_info) > 6):
            broker = f'{broker}-{"-".join(dir_info[6:])}'
        pub_throughput = np.loadtxt(
            pub_file_path, delimiter=',', skiprows=1, dtype=np.int64)
        if broker not in result.keys():
            result[broker] = {}
        result[broker][message_size] = pub_throughput
    return result


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


def plot_bar_from_key(root_dir, throughput_map, mode, keys, **kwargs):
    fig_msg, ax_msg = plt.subplots()
    fig_byte, ax_byte = plt.subplots()
    match_keys = [key for key in throughput_map.keys() if key in keys]
    width = 0.4
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
            mean_msg.append(throughput[2:, 1].mean())
            std_msg.append(throughput[2:, 1].std())
            mean_byte.append((throughput[2:, 2]/1000/1000).mean())
            std_byte.append((throughput[2:, 2]/1000/1000).std())
            if (is_first):
                labels.append(sz)
        is_first = False

    sz_num = len(labels)
    for i, label in enumerate(labels):
        ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
                   yerr=np.array(std_msg[i::sz_num]), width=width, capsize=2, label=label)
        ax_byte.bar(x[i::sz_num], np.array(mean_byte[i::sz_num]),
                    yerr=np.array(std_byte[i::sz_num]), width=width, capsize=2, label=label)

    if ('title' in kwargs.keys()):
        ax_msg.set_title(kwargs['title'])
    else:
        ax_msg.set_title("_".join(match_keys))
    ax_msg.set_ylabel("Throughput[msg/s]")
    ax_msg.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax_msg.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax_msg.set_xticklabels(x_ticklabels)
    ax_msg.legend()
    fig_msg.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_msg.png'))

    if ('title' in kwargs.keys()):
        ax_byte.set_title(kwargs['title'])
    else:
        ax_byte.set_title("_".join(match_keys))
    ax_byte.set_ylabel("Throughput[MB/s]")
    ax_byte.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax_byte.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax_byte.set_xticklabels(x_ticklabels)
    ax_byte.legend()
    fig_byte.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_byte.png'))


def plot_single():
    root_dir = 'results/2022_0603_100b_16kb'
    pub_throughput_map = collect_throughput(root_dir, "**/*publisher*throuput*")
    # plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-acks-0", "kafka-acks-1"])
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-acks-0", "kafka-acks-1", "jetstream", "nats"])

    sub_throughput_map = collect_throughput(root_dir, "**/*subscriber*throuput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ["kafka-acks-0", "kafka-acks-1"])
    plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ["kafka-acks-0", "kafka-acks-1", "jetstream", "nats"])

    for broker, pub_throughput in pub_throughput_map.items():
        throughput_map = {}
        throughput_map['pub_' + broker] = pub_throughput
        throughput_map['sub_' + broker] = sub_throughput_map[broker]
        plot_bar_from_key(root_dir, throughput_map, 'pub-sub', ['pub_'+broker, 'sub_'+broker])



def plot_single_kafka():
    root_dir = 'results/2022_0604_kafka_buff_100b_16kb'
    pub_throughput_map = collect_throughput(root_dir, "**/*publisher*throuput*")
    plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", [
                      "kafka-acks-0", "kafka-acks-1", "kafka-acks-0-buff-32k-linger-10", "kafka-acks-1-buff-32k-linger-10"], 
                      title="Kafka when changing parameters", 
                      xtickslabels=["ack=0", "ack=1", "ack=0\nch params", "ack=1\nch params"])

    # sub_throughput_map = collect_throughput(root_dir, "**/*subscriber*throuput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", [
    #                   "kafka-acks-0", "kafka-acks-1", "kafka-acks-0-buff-32k-linger-10", "kafka-acks-1-buff-32k-linger-10"], 
    #                   title="Kafka when changing parameters", 
    #                   xtickslabels=["ack=0", "ack=1", "ack=0\nch params", "ack=1\nch params"])

def plot_cluster():
    root_dir = 'results/2022_0606_cluster'
    pub_throughput_map = collect_throughput(root_dir, "**/*publisher*throuput*")
    pub_cl_throughput_map = collect_throughput(root_dir, "**/*publisher*throughput*")
    # plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    # plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-acks-0-cl-3", "kafka-acks-1-cl-3", "jetstream-cl-3"])


    sub_throughput_map = collect_throughput(root_dir, "**/*subscriber*throuput*")
    sub_cl_throughput_map = collect_throughput(root_dir, "**/*subscriber*throughput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ["kafka-acks-0-cl-3", "kafka-acks-1-cl-3", "jetstream-cl-3"])

    for broker, pub_throughput in pub_throughput_map.items():
        if len(broker.split('-')) > 3:
            continue
        throughput_map = {}
        throughput_map['pub-'+broker] = pub_throughput_map[broker]
        throughput_map['sub-'+broker] = sub_throughput_map[broker]
        throughput_map['pub-'+broker+'-cl-3'] = pub_cl_throughput_map[broker+'-cl-3']
        throughput_map['sub-'+broker+'-cl-3'] = sub_cl_throughput_map[broker+'-cl-3']
        plot_bar_from_key(root_dir, throughput_map, 'pub-sub', throughput_map.keys(),
            title=broker+' single and cluster', xtickslabels=['pub-single', 'sub-single', 'pub-cluster', 'sub-cluster'])


if __name__ == '__main__':
    # plot_single()
    # plot_single_kafka()
    plot_cluster()