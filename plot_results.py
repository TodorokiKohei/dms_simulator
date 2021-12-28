import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


result_dir = os.path.join('results', '20211228_2349')
# result_dir = os.path.join('results', '20211229_0040')

messages_file = os.path.join(result_dir, 'sub-1_client_0_messages.csv')
df_msg = pd.read_csv(messages_file, parse_dates=[2,3])
publisher_file = os.path.join(result_dir, 'pub-1_client_0_results.csv')
df_pub = pd.read_csv(publisher_file, parse_dates=[0])
subscriber_file = os.path.join(result_dir, 'sub-1_client_0_results.csv')
df_sub = pd.read_csv(subscriber_file, parse_dates=[0])

df_msg['sent_time'] = df_msg['sent_time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
df_msg['receive_time'] = df_msg['receive_time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
df_msg['time_from_sent_start'] = (df_msg['sent_time'] - df_msg['sent_time'][0]).dt.total_seconds()
df_msg['time_from_recv_start'] = (df_msg['receive_time'] - df_msg['receive_time'][0]).dt.total_seconds()

# 1行目を削除
df_msg.drop(0, axis=0)
df_pub.drop(0, axis=0)
df_sub.drop(0, axis=0)

latency = (df_msg['receive_time'] - df_msg['sent_time']).dt.total_seconds()
plt.plot(df_msg['time_from_sent_start'], latency)
plt.ylabel('latency [sec]')
plt.xlabel('elapsed time [sec]')
# plt.ylim([0, 10])
plt.savefig(os.path.join(result_dir, 'latency.png'))
plt.cla()

# sr_sent = df_msg['sent_time'].map(lambda x: x.replace(microsecond=0)).value_counts(sort=False)
# plt.plot(sr_sent.index, sr_sent.values)
# plt.savefig(os.path.join(result_dir, 'pub_rate.png'))
# plt.cla()
# sr_receive = df_msg['receive_time'].map(lambda x: x.replace(microsecond=0)).value_counts(sort=False)
# plt.plot(sr_receive.index, sr_receive.values)
# plt.savefig(os.path.join(result_dir, 'sub_rate.png'))
# plt.cla()

plt.plot((df_pub['LOGTIME']-df_pub['LOGTIME'][0]).dt.total_seconds(), df_pub['MESSAGE_BYTES_TOTAL'])
plt.ylabel('publish rate [bps]')
plt.xlabel('elapsed time [sec]')
# plt.ylim([0, 110000])
plt.savefig(os.path.join(result_dir, 'pub_rate.png'))
plt.cla()

plt.plot((df_sub['LOGTIME']-df_sub['LOGTIME'][0]).dt.total_seconds(), df_sub['MESSAGE_BYTES_TOTAL'])
plt.ylabel('subscribe rate [bps]')
plt.xlabel('elapsed time [sec]')
# plt.ylim([0, 110000])
plt.savefig(os.path.join(result_dir, 'sub_rate.png'))
plt.cla()

pub_message_diff = df_msg['sent_time'].diff().dropna().dt.total_seconds()
plt.plot(df_msg['time_from_sent_start'].iloc[:-1], pub_message_diff)
plt.ylabel('message interval [sec]')
plt.xlabel('elapsed time [sec]')
plt.savefig(os.path.join(result_dir, 'pub_message_diff.png'))
plt.cla()

sub_message_diff = df_msg['receive_time'].diff().dropna().dt.total_seconds()
plt.plot(df_msg['time_from_recv_start'].iloc[:-1], sub_message_diff)
plt.ylabel('message interval [sec]')
plt.xlabel('elapsed time [sec]')
# plt.ylim([0, 5])
plt.savefig(os.path.join(result_dir, 'sub_message_diff.png'))
plt.cla()