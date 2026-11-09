# 2026-11-06 两轮各两小时 durable 压测

## 目标与边界

按已验证的最佳组合串行运行 put+evict、put+remove，每轮供压 7,200 秒。
每轮重新建立专用 etcd 集群、master 和两个 standby，采用不同 cluster id 和数据目录。
第二轮结束后停止本轮服务，保存报告、数值审计、压缩事件日志，删除已经停止的本轮 etcd 数据目录。
不操作系统 etcd 的 2379/2380 端口，也不操作其他用户的服务。

master 为 madsys-02（10.1.100.8），standby 为 madsys-01（10.1.100.3）和 madsys-03（10.1.101.3）。
专用 etcd 三节点端口为 12479/12480；master RPC 为 45151，metrics 为 49103，HTTP metadata 为 48180。
供压在 madsys-03。master 复用之前在 madsys 构建并验证的二进制；新增供压程序仅在 madsys-01 串行编译。

## 共同配置

| 项目 | 配置 |
|---|---|
| 供压 | 2 workers；每次 BatchPutStart/BatchPutEnd 最多 128 对象；不设置固定速率上限 |
| 对象与 segment | 1KiB 对象；单副本；256MiB 虚拟 segment；无实际数据面内存分配 |
| key/segment 命名 | run_id 前缀固定为 82 个 ASCII 字节，与上一轮 r10m3 一致；两轮使用相同长度，防止名称变化改变 OpLog 编码量 |
| 验证 | 每 16 个 batch 检查地址、长度、副本标识、状态和 checksum 元数据 |
| master | HA、OpLog、Snapshot 全开；RPC 线程 16；offload_on_evict=false |
| OpLog | 每 batch 最多 8192 entries；standby 每次最多读取 16 batch；已有 Serializable Range 配置随验证过的二进制保留 |
| 历史 OpLog 删除 | 每块最多 10 batch；最多 50 batch/s |
| Snapshot | shared local/NFS；间隔 60 秒；每 chunk 100000 对象；部署前跨三节点验证同一共享目录 |
| etcd | 3.5.33；periodic retention 5m；compaction limit=8，sleep=100ms |
| etcd backend | interval=100ms；limit=10000；snapshot-count=1000；quota=24GiB；maxrequest=8MiB |

第一轮只发 BatchPut，依靠 master eviction 回收。第二轮每个 worker 保留最多约 32768 个成功 key，总目标 65536 个（64MiB）；达到窗口后先 BatchRemove，再补入 BatchPut。删除失败的 key 保持占用窗口并重试，防止静默丢弃导致存量上涨。由于部分 batch 返回成功数量不固定，总窗口允许少于一个 batch 的波动；与 256MiB 容量之间仍有充分余量。第二轮只删除本轮生成的 key。

## 执行

在本目录运行：

```bash
# 构建供压程序、核对三节点二进制；不重编译 master。
python3 -u prepare.py > prepare.log 2>&1

# 统计与进程身份回归检查。
python3 -m unittest test_statistics.py test_process_identity.py

# 短时两场景试跑，label 必须唯一。
python3 -u run_suite.py --seconds 60 --label smoke02 > smoke02.log 2>&1

# 正式两轮顺序执行；正式启动时由独立会话托管，不依赖终端保持连接。
python3 -u run_suite.py --seconds 7200 --label long02 > long02.log 2>&1
```

程序用 `suite.lock` 排斥重叠测试；每个进程保存 PID、启动时间与完整命令，停止时逐项核对。
启动初期 `/proc/PID/cmdline` 可能短暂为空，登记身份须等待非空值，不能把空命令记成已退出。
本地控制器每 15 秒查询状态，远端每秒采集指标；SSH 连接异常最多容忍 10 分钟。
可用内存低于 6GiB、根盘剩余低于 6GiB、专用 etcd DB 超过 18GiB 时停止本轮并保留原因。
发生主从角色变化、元数据位置不一致、审计缺口等，也明确记录异常，不以缩短后的窗口冒充完整两小时。

## durable 口径

etcd watch 同时观察 OpLog batch 和 durable_prefix；仅在 batch 与 prefix 的 revision 和 last_seq 一致时计数。
连续检查 batch id、entry sequence、各类型计数和最终 durable_prefix。OpType 1 是 PUT_END，3 是 REMOVE。
REMOVE 在第一轮主要来自 eviction，在第二轮主要来自显式删除；另列 master eviction 计数核对是否混入淘汰。
成功 PutEnd 回调数量不是 durable 数量，HA-002 的差异按既定设计保留。

逐分钟窗口以客户端开始时间为原点，左闭右开，每轮正常为 120 行。
batch/s = 该分钟 batch 数 / 60，不混淆为对象数/秒。
供压停止后的收尾 entry 单独列示，不混入 7200 秒分母。
按 watch 到达并确认的时刻归窗，存在网络和观察延迟；这是分钟粒度的归属边界，不是服务端的精确提交时间戳。

## 产物与字段

每轮目录包含 `REPORT.md`、`per-minute.csv`、`ISSUES.md`、`summary.json`、`run.json` 和实际启动配置。
CSV 为 UTF-8 BOM、逗号分隔；字段名稳定使用 ASCII，中文含义如下：

| 字段/前缀 | 含义 |
|---|---|
| minute / window_start_cst / window_seconds | 分钟序号、北京时间起点、窗口长度 |
| supply_seconds / audit_complete | 本分钟实际供压时长、全程审计是否对齐最终 prefix |
| durable_put_end / durable_remove / durable_entries | 本分钟确认 durable 的对象类型数量和全部 entries |
| durable_batches / entries_per_batch / oplog_bytes | 本分钟 batch 数、平均 entries/batch、编码字节数 |
| *_per_s | 对应窗口数量除以秒数 |
| madsys-*_encode_* / backend_txn_* / commit_to_durable_* | 每节点相应阶段延迟，单位 ms |
| *_p95_upper_ms / *_p99_upper_ms | 本分钟直方图差分的分位数桶上界；inf 为溢出最高有限桶 |
| *_txn_over_100ms | backend Txn 超过 100ms 的计数增量 |
| *_wal_fsync_* / backend_commit_* / compact_pause_* | etcd 相应阶段的延迟 |
| *_ha_oplog_standby_lag_* / applied_sequence_id_* | standby 落后量与回放进度 |
| *_snapshot_* / compaction_floor_* | Snapshot 发布、错误、清理进度等 |
| *_etcd_mvcc_db_* / *_revision_* | etcd DB 体积、使用量与 revision |
| *_cpu_cores_mean / rss_bytes_max | 进程平均占用 CPU 核数、最大 RSS bytes |
| *_available_bytes_min / disk_free_bytes_min | 节点本分钟最低内存/磁盘余量 |
| *_samples / sample_span_s / scrape_errors | 采样覆盖与错误；缺失项不作为已验证的零值 |
| client_* | 辅助解释供压和校验行为；不替代 master durable 指标 |

异常报告区分已确认事实与待查原因。Txn 超过 100ms、Snapshot 错误、主从切换、显式 Remove 场景发生淘汰、审计或采集不完整均记录；不依据时间重合直接断言因果。
