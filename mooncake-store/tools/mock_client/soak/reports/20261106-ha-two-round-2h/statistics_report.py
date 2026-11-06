"""从审计及累积指标生成可复算的逐分钟 CSV；PUT_END=1，REMOVE=3。"""

import csv
import datetime as dt
import gzip
import json
import math
from pathlib import Path
import re
import sys

HOSTS = ("madsys-01", "madsys-02", "madsys-03")


def parse_metrics(text):
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(None, 1)
        if len(parts) != 2:
            continue
        try:
            out[parts[0]] = float(parts[1])
        except ValueError:
            pass
    return out


def histogram_delta(before, after, name):
    count = after.get(name + "_count", 0) - before.get(name + "_count", 0)
    total = after.get(name + "_sum", 0) - before.get(name + "_sum", 0)
    result = {"count": count, "mean": None, "p95_upper": None, "p99_upper": None}
    if count <= 0 or total < 0:
        return result
    result["mean"] = total / count
    buckets = []
    for k, v in after.items():
        if not k.startswith(name + "_bucket{"):
            continue
        match = re.search(r'le="([^"]+)"', k)
        if match:
            buckets.append((float(match[1]), v - before.get(k, 0)))
    for q in (0.95, 0.99):
        for bound, n in sorted(buckets):
            if n >= count * q:
                result["p" + str(int(q * 100)) + "_upper"] = (
                    "inf" if math.isinf(bound) else bound
                )
                break
    return result


def counter_delta(before, after, key):
    if key not in before or key not in after:
        return None
    value = after[key] - before[key]
    return value if value >= 0 else None


def audit_minutes(rows, start, seconds):
    minutes = [
        dict(
            minute=i + 1,
            window_start_epoch=start + i * 60,
            window_seconds=min(60, seconds - i * 60),
            durable_put_end=0,
            durable_remove=0,
            durable_other=0,
            durable_entries=0,
            durable_batches=0,
            oplog_bytes=0,
        )
        for i in range(math.ceil(seconds / 60))
    ]
    total = dict(
        last_seq=0,
        last_batch=0,
        tail_entries=0,
        tail_put_end=0,
        tail_remove=0,
        pruned_batches_observed=0,
        all_entries=0,
        all_put_end=0,
        all_remove=0,
    )
    for r in rows:
        if (
            r["batch"] != total["last_batch"] + 1
            or r["first"] != total["last_seq"] + 1
            or r["last"] - r["first"] + 1 != r["entries"]
            or sum(r["counts"].values()) != r["entries"]
        ):
            raise ValueError(
                "审计batch/seq/count不连续或不一致：" + str(r.get("batch"))
            )
        put = r["counts"].get("1", 0)
        remove = r["counts"].get("3", 0)
        total.update(
            last_seq=r["last"],
            last_batch=r["batch"],
            pruned_batches_observed=r.get("deleted", 0),
        )
        total["all_entries"] += r["entries"]
        total["all_put_end"] += put
        total["all_remove"] += remove
        index = int((r["time"] - start) // 60)
        if index < 0:
            continue
        if r["time"] >= start + seconds:
            total["tail_entries"] += r["entries"]
            total["tail_put_end"] += put
            total["tail_remove"] += remove
            continue
        m = minutes[index]
        m["durable_put_end"] += put
        m["durable_remove"] += remove
        m["durable_other"] += r["entries"] - put - remove
        m["durable_entries"] += r["entries"]
        m["durable_batches"] += 1
        m["oplog_bytes"] += r["bytes"]
    for m in minutes:
        for key in [
            "durable_put_end",
            "durable_remove",
            "durable_entries",
            "durable_batches",
        ]:
            m[key + "_per_s"] = m[key] / m["window_seconds"]
        m["entries_per_batch"] = (
            m["durable_entries"] / m["durable_batches"]
            if m["durable_batches"]
            else None
        )
        m["oplog_bytes_per_s"] = m["oplog_bytes"] / m["window_seconds"]
        m["window_start_cst"] = dt.datetime.fromtimestamp(
            m["window_start_epoch"], dt.timezone(dt.timedelta(hours=8))
        ).isoformat()
    return minutes, total


def jsonlines(path):
    if not path.exists():
        return
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def node_minutes(path, start, seconds):
    """流式聚合，只持有一个分钟窗口，避免长测统计本身占用GB内存。"""
    output = {}
    bucket = []
    index = None
    previous = None

    def finish(i, rows, prior):
        if not rows:
            return
        first = prior or rows[0]
        last = rows[-1]
        a = first.get("metrics", {})
        b = last.get("metrics", {})
        ea = first.get("etcd", {})
        eb = last.get("etcd", {})
        dt_s = last["time"] - first["time"]
        out = {
            "samples": len(rows),
            "sample_span_s": round(dt_s, 3),
            "scrape_errors": sum(len(r.get("errors", {})) for r in rows),
        }
        for key in ["available_bytes", "disk_free_bytes"]:
            v = [r[key] for r in rows if key in r]
            out[key + "_min"] = min(v) if v else None
        for key in [
            "master_key_count",
            "master_allocated_bytes",
            "master_total_capacity_bytes",
            "ha_batch_record_durable_sequence",
            "ha_batch_record_committed_queue_depth",
            "ha_batch_record_callback_queue_depth",
            "ha_oplog_standby_lag",
            "ha_oplog_applied_sequence_id",
            "ha_snapshot_latest_batch",
            "ha_snapshot_compaction_floor",
            "ha_snapshot_count",
            "mooncake_ha_batch_oplog_pruner_floor_lag_batch_ids",
        ]:
            out[key + "_last"] = b.get(key)
            values = [r.get("metrics", {}).get(key) for r in rows]
            values = [x for x in values if x is not None]
            out[key + "_max"] = max(values) if values else None
        for key in [
            "master_evicted_key_count",
            "master_batch_put_end_items_total",
            "master_batch_put_end_failed_items_total",
            "ha_batch_record_durable_entries_total",
            "ha_batch_record_retry_total",
            "ha_oplog_checksum_failures_total",
            "ha_snapshot_lease_lost_total",
            "mooncake_ha_batch_oplog_pruner_batch_ids_advanced_total",
            "mooncake_ha_batch_oplog_pruner_errors_total",
        ]:
            out[key + "_delta"] = counter_delta(a, b, key)
        for op in [
            "publish",
            "prune",
            "rebootstrap",
            "upload",
            "schedule",
            "bootstrap",
            "replay",
            "gc",
        ]:
            for kind in ["operations", "errors"]:
                key = "ha_snapshot_" + kind + '_total{operation="' + op + '"}'
                out["snapshot_" + op + "_" + kind + "_delta"] = counter_delta(a, b, key)
        for tag, key in [
            ("encode", "ha_batch_record_encode_latency_us"),
            ("decode", "ha_batch_record_decode_latency_us"),
            ("backend_txn", "ha_batch_record_backend_txn_latency_us"),
            ("txn", "ha_batch_record_txn_latency_us"),
            ("commit_to_durable", "ha_batch_record_commit_to_durable_us"),
            ("callback", "ha_batch_record_callback_latency_us"),
        ]:
            hist = histogram_delta(a, b, key)
            out[tag + "_count"] = hist["count"]
            for stat in ["mean", "p95_upper", "p99_upper"]:
                v = hist[stat]
                out[tag + "_" + stat + "_ms"] = (
                    v / 1000 if isinstance(v, (int, float)) else v
                )
            if tag == "backend_txn":
                k = key + '_bucket{le="100000.000000"}'
                out["txn_over_100ms"] = (
                    hist["count"] - (b.get(k, 0) - a.get(k, 0))
                    if hist["count"] >= 0
                    else None
                )
        for tag, key, scale in [
            ("wal_fsync", "etcd_disk_wal_fsync_duration_seconds", 1000),
            ("backend_commit", "etcd_disk_backend_commit_duration_seconds", 1000),
            (
                "compact_pause",
                "etcd_debugging_mvcc_db_compaction_pause_duration_milliseconds",
                1,
            ),
        ]:
            hist = histogram_delta(ea, eb, key)
            out[tag + "_count"] = hist["count"]
            for stat in ["mean", "p95_upper", "p99_upper"]:
                v = hist[stat]
                out[tag + "_" + stat + "_ms"] = (
                    v * scale if isinstance(v, (int, float)) else v
                )
        for key in [
            "etcd_mvcc_db_total_size_in_bytes",
            "etcd_mvcc_db_total_size_in_use_in_bytes",
            "etcd_debugging_mvcc_current_revision",
            "etcd_debugging_mvcc_compact_revision",
        ]:
            out[key + "_last"] = eb.get(key)
        for key in [
            "etcd_server_leader_changes_seen_total",
            "etcd_server_proposals_failed_total",
            "etcd_debugging_mvcc_db_compaction_keys_total",
            "etcd_debugging_mvcc_db_compaction_total_duration_milliseconds_count",
            "etcd_debugging_mvcc_db_compaction_total_duration_milliseconds_sum",
        ]:
            out[key + "_delta"] = counter_delta(ea, eb, key)
        for proc in ["master", "etcd", "client"]:
            ps = [r.get("processes", {}).get(proc, {}) for r in rows]
            rss = [r["rss_bytes"] for r in ps if "rss_bytes" in r]
            out[proc + "_rss_bytes_max"] = max(rss) if rss else None
            p0 = first.get("processes", {}).get(proc, {})
            p1 = last.get("processes", {}).get(proc, {})
            if "cpu_seconds" in p0 and "cpu_seconds" in p1 and dt_s > 0:
                out[proc + "_cpu_cores_mean"] = (
                    p1["cpu_seconds"] - p0["cpu_seconds"]
                ) / dt_s
                out[proc + "_write_bytes_delta"] = p1.get("io", {}).get(
                    "write_bytes", 0
                ) - p0.get("io", {}).get("write_bytes", 0)
        for key in [
            "putend_queue_rejected",
            "remove_missing_warning",
            "evict_backpressure",
        ]:
            out[key + "_delta"] = last.get("master_log_counts", {}).get(
                key, 0
            ) - first.get("master_log_counts", {}).get(key, 0)
        health = [r.get("health", {}) for r in rows]
        out["health_roles"] = "|".join(
            sorted(set(str(h.get("role", "unknown")) for h in health))
        )
        out["health_view_versions"] = "|".join(
            sorted(set(str(h.get("view_version", "unknown")) for h in health))
        )
        output[i] = out

    for row in jsonlines(path):
        if not isinstance(row.get("metrics", {}), dict):
            row["metrics"] = parse_metrics(row.get("metrics", ""))
        if not isinstance(row.get("etcd", {}), dict):
            row["etcd"] = parse_metrics(row.get("etcd", ""))
        t = row["time"]
        if t < start:
            previous = row
            continue
        if t >= start + seconds:
            break
        i = int((t - start) // 60)
        if index is not None and i != index:
            finish(index, bucket, previous)
            previous = bucket[-1]
            bucket = []
        index = i
        bucket.append(row)
    if index is not None:
        finish(index, bucket, previous)
    return output


def write_csv(path, rows):
    columns = list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, columns)
        writer.writeheader()
        writer.writerows(rows)


def generate(root):
    root = Path(root)
    run = json.loads((root / "run.json").read_text())
    client = list(jsonlines(root / "madsys-03/client.jsonl"))
    if not client:
        (root / "ISSUES.md").write_text(
            "# 本轮测试问题\n\n未采集到 client 起止记录，不能生成有效吞吐结果。\n"
            + str(run.get("error", ""))
            + "\n"
        )
        return
    start = client[0]["time"]
    target = run["duration"]
    done = next((r for r in reversed(client) if r.get("phase") == "done"), client[-1])
    actual = done.get("elapsed", done["time"] - start)
    audit_error = None
    try:
        minutes, total = audit_minutes(
            jsonlines(root / "madsys-01/audit.jsonl.gz"), start, target
        )
    except Exception as e:
        audit_error = repr(e)
        minutes, total = audit_minutes([], start, target)
    end = (run.get("final_boundary") or {}).get("last_seq")
    audit_complete = end is not None and end == total["last_seq"] and not audit_error
    for m in minutes:
        m["audit_complete"] = int(audit_complete)
        m["supply_seconds"] = round(
            max(0, min(m["window_seconds"], actual - (m["minute"] - 1) * 60)), 3
        )
        m["workload"] = run["workload"]
    for host in HOSTS:
        for i, data in node_minutes(
            root / host / "monitor.jsonl.gz", start, target
        ).items():
            minutes[i].update({host + "_" + k: v for k, v in data.items()})
    prev = client[0]
    for m in minutes:
        bound = m["window_start_epoch"] + m["window_seconds"]
        current = next((r for r in reversed(client) if r["time"] < bound), prev)
        for key in [
            "success",
            "attempted",
            "start_errors",
            "end_errors",
            "remove_success",
            "remove_errors",
            "verified",
            "mismatch",
            "unknown",
            "missing",
        ]:
            m["client_" + key] = current.get(key, 0) - prev.get(key, 0)
        m["client_live_keys_last"] = current.get("live_keys", 0)
        prev = current
    write_csv(root / "per-minute.csv", minutes)
    summary = dict(
        total,
        requested_seconds=target,
        actual_seconds=actual,
        audit_complete=audit_complete,
        audit_error=audit_error,
        final_boundary=run.get("final_boundary"),
        client=done,
        client_rc=run.get("client_rc"),
    )
    summary["window_put_end"] = sum(m["durable_put_end"] for m in minutes)
    summary["window_remove"] = sum(m["durable_remove"] for m in minutes)
    summary["window_batches"] = sum(m["durable_batches"] for m in minutes)
    summary["evicted_keys"] = sum(
        m.get("madsys-02_master_evicted_key_count_delta") or 0 for m in minutes
    )
    summary["put_per_s"] = summary["window_put_end"] / min(target, max(1, actual))
    summary["remove_per_s"] = summary["window_remove"] / min(target, max(1, actual))
    summary["txn_over_100ms"] = sum(
        m.get("madsys-02_txn_over_100ms") or 0 for m in minutes
    )
    log_counts_path = root / "madsys-02/master-log-counts.json"
    log_counts = (
        json.loads(log_counts_path.read_text()).get("counts", {})
        if log_counts_path.exists()
        else {}
    )
    summary["putend_queue_rejected"] = log_counts.get("putend_queue_rejected")
    if summary["putend_queue_rejected"] is not None:
        summary["callback_reconciliation_difference"] = (
            done.get("success", 0)
            - summary["putend_queue_rejected"]
            - total["all_put_end"]
        )
    warnings = []
    if actual < target - 1 or run.get("client_rc") != 0:
        warnings.append(
            (
                "测试未完成目标时长",
                "测试工具/集群",
                f'目标 {target}s，实际 {actual:.1f}s；client rc={run.get("client_rc")}；控制器错误={run.get("error")}',
                "检查 run.json 和 GUARD/audit-error/进程退出记录；不将未完成标为通过。",
            )
        )
    if not audit_complete:
        warnings.append(
            (
                "durable 审计边界未对齐",
                "测试工具",
                f'最终 prefix={end}，审计 seq={total["last_seq"]}，解析错误={audit_error}',
                "先补齐审计，不使用不完整数字声称性能提升。",
            )
        )
    if summary.get("callback_reconciliation_difference"):
        warnings.append(
            (
                "callback/入队失败/durable 未对齐",
                "OpLog/测试工具",
                f'callback={done.get("success")}，PutEnd入队失败={summary["putend_queue_rejected"]}，完整durable PUT_END={total["all_put_end"]}，剩余差异={summary["callback_reconciliation_difference"]}。',
                "先检查日志计数覆盖、收尾及错误类型；不把无法解释的差值直接归为HA-002。",
            )
        )
    if summary["txn_over_100ms"] > 0:
        warnings.append(
            (
                "durable Txn 仍超过100ms",
                "OpLog/etcd",
                f'供压期间直方图显示超过100ms的backend Txn约 {summary["txn_over_100ms"]} 次。',
                "结合每分钟compaction、DB、CPU、standby和WAL/backend指标定位；单凭共时性不认定因果。",
            )
        )
    if run["workload"] == "put_remove" and summary["evicted_keys"] > 0:
        warnings.append(
            (
                "显式 Remove 场景仍发生 eviction",
                "Store/测试工具",
                f'监控增量 eviction keys={summary["evicted_keys"]}；目标 live keys=65536。',
                "检查待删除队列、容量和Remove延迟；该窗口不能作为无eviction对照。",
            )
        )
    for host in HOSTS:
        for name in ["GUARD.json", "WATCHDOG.json", "audit-error.json"]:
            p = root / host / name
            if p.exists():
                obj = json.loads(p.read_text())
                warnings.append(
                    (
                        host + " " + name,
                        "测试工具/资源",
                        json.dumps(
                            {k: v for k, v in obj.items() if k != "row"},
                            ensure_ascii=False,
                        ),
                        "保留边界证据；原因依据保护阈值及每分钟资源数据核验。",
                    )
                )
        errors = sum(
            sum(
                (m.get(host + "_snapshot_" + op + "_errors_delta") or 0)
                for op in [
                    "publish",
                    "upload",
                    "prune",
                    "rebootstrap",
                    "schedule",
                    "bootstrap",
                    "replay",
                    "gc",
                ]
            )
            for m in minutes
        )
        if errors:
            warnings.append(
                (
                    host + " Snapshot操作错误",
                    "Snapshot/HA",
                    f"采样累计错误增量={errors}。",
                    "核对事件日志具体错误及后续重试是否恢复。",
                )
            )
        leader_changes = sum(
            m.get(host + "_etcd_server_leader_changes_seen_total_delta") or 0
            for m in minutes
        )
        if leader_changes:
            warnings.append(
                (
                    host + " etcd切主",
                    "etcd",
                    f"leader_changes增量={leader_changes}。",
                    "对齐资源/心跳与compaction，不仅凭次数判断原因。",
                )
            )
    if done.get("mismatch", 0) or done.get("unknown", 0):
        warnings.append(
            (
                "客户端元数据验证或连接异常",
                "Store/HA/测试工具",
                json.dumps(done.get("errors", {}))
                + f'；mismatch={done.get("mismatch")}，unknown={done.get("unknown")}',
                "按时间对齐服务健康及日志；错误位置不得按HA-002忽略。",
            )
        )
    summary["unexpected_findings"] = len(warnings)
    (root / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    )
    report = f"""# {run['workload']} durable 性能测试报告

目标供压时长 {target} 秒，实际 {actual:.3f} 秒，client rc={run.get('client_rc')}。
最终审计是否与专用 etcd durable_prefix 对齐：**{audit_complete}**。

| 供压窗口指标 | 数量 | 平均每秒 |
|---|---:|---:|
| durable PUT_END | {summary['window_put_end']} | {summary['put_per_s']:.3f} |
| durable REMOVE | {summary['window_remove']} | {summary['remove_per_s']:.3f} |
| durable batch | {summary['window_batches']} | {summary['window_batches']/min(target,max(1,actual)):.3f} |
| master eviction keys（采样增量） | {summary['evicted_keys']} | — |

完整审计：batch={total['last_batch']}，seq={total['last_seq']}；包含收尾的 PUT_END={total['all_put_end']}，REMOVE={total['all_remove']}。
结束后的额外 entry={total['tail_entries']}，单独记录，不混入供压窗口。
client 成功回调={done.get('success')}，Remove成功={done.get('remove_success',0)}；校验={done.get('verified')}，位置/校验不一致={done.get('mismatch')}，missing={done.get('missing')}。
HA-002的callback/durable差异作为既定语义记录，不改生产代码掩盖差异。
最终日志记录的 PutEnd 入队失败={summary['putend_queue_rejected']}；callback 减入队失败再减完整 durable PUT_END 的剩余差异={summary.get('callback_reconciliation_difference')}。

配置：2 workers，client batch=128，1KiB对象，单副本，256MiB模拟segment；put_remove保留65536个成功key的存量窗口，先删除再补入；HA+OpLog+Snapshot开启，master=madsys-02，standby=madsys-01/03，RPC线程16，OpLog batch上限8192，standby每次最多读取16 batch，prune chunk=10/max=50 batch/s，Snapshot间隔60s/chunk100000，offload_on_evict=false。etcd三节点3.5.33、periodic保留5m、compaction limit=8/sleep=100ms、backend interval=100ms/limit10000、snapshot-count1000、quota24GiB/maxrequest8MiB。
二进制来源及SHA256见 build-manifest.json，逐节点实际启动命令见 deployment.json；两轮复用相同二进制，每轮使用新的专用etcd数据与cluster id。

逐分钟统计在 `per-minute.csv`（UTF-8带BOM，逗号分隔），共{len(minutes)}行。窗口以client开始时间为原点，左闭右开；durable按审计确认时间归窗，存在watch传输/采集延迟，并非服务端精确commit时间。数值0表示未观察到事件，缺失的监控字段留空；`samples`、`sample_span_s`和`supply_seconds`用于判断窗口是否完整。发生审计失败时，全部durable统计应视为无效，不能把零当作正常吞吐。
延迟p95/p99是累积直方图差分的桶上界，不是逐RPC精确分位数；`inf`表示溢出最高有限桶，不能解读为实际无限延迟。CPU以核数计，内存/磁盘以bytes计；入队失败和standby缺失Remove告警按接收计数，可能相对真实操作稍有延迟。
重复日志只保留每分钟次数和样本，以免测试日志耗尽磁盘。原始每秒数值监控、每batch审计、压缩事件日志、最终prefix和etcd状态均保留供复算。

发现 {len(warnings)} 项需分析的非预期表现，详见 `ISSUES.md`。是否存在Txn长尾：超过100ms计数={summary['txn_over_100ms']}。
"""
    report += "\n## 每十五分钟吞吐变化\n\n| 分钟 | durable PUT_END/s | durable REMOVE/s | durable 全部entries/s | batch/s | Txn >100ms次数 |\n|---|---:|---:|---:|---:|---:|\n"
    for first in range(0, len(minutes), 15):
        part = minutes[first : first + 15]
        duration = sum(m["window_seconds"] for m in part)
        report += (
            "| "
            + str(first + 1)
            + "–"
            + str(first + len(part))
            + " | "
            + " | ".join(
                f"{sum(m[k] for m in part)/duration:.3f}"
                for k in [
                    "durable_put_end",
                    "durable_remove",
                    "durable_entries",
                    "durable_batches",
                ]
            )
            + " | "
            + str(sum(m.get("madsys-02_txn_over_100ms") or 0 for m in part))
            + " |\n"
        )
    report += "\n## 各节点资源和 Snapshot\n\n| 节点 | 最低可用内存 GiB | 最低磁盘余量 GiB | 最大 DB GiB | 最大 master RSS GiB | 最大 standby lag entries | 发布次数 | Snapshot错误次数 | 最后清理floor |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n"

    def aggregate(host, key, fn):
        values = [m.get(host + "_" + key) for m in minutes]
        values = [v for v in values if isinstance(v, (int, float))]
        return fn(values) if values else None

    for host in HOSTS:
        values = [
            aggregate(host, k, fn)
            for k, fn in [
                ("available_bytes_min", min),
                ("disk_free_bytes_min", min),
                ("etcd_mvcc_db_total_size_in_bytes_last", max),
                ("master_rss_bytes_max", max),
                ("ha_oplog_standby_lag_max", max),
                ("snapshot_publish_operations_delta", sum),
            ]
        ]
        cells = [
            f"{v/1024**3:.3f}" if v is not None else "缺失" for v in values[:4]
        ] + [str(v) if v is not None else "缺失" for v in values[4:]]
        errors = sum(
            sum(
                m.get(host + "_snapshot_" + op + "_errors_delta") or 0
                for op in [
                    "publish",
                    "upload",
                    "prune",
                    "rebootstrap",
                    "schedule",
                    "bootstrap",
                    "replay",
                    "gc",
                ]
            )
            for m in minutes
        )
        floor = next(
            (
                m.get(host + "_ha_snapshot_compaction_floor_last")
                for m in reversed(minutes)
                if m.get(host + "_ha_snapshot_compaction_floor_last") is not None
            ),
            None,
        )
        report += (
            "| " + host + " | " + " | ".join(cells + [str(errors), str(floor)]) + " |\n"
        )
    report += "\n## durable 路径平均耗时\n\n| 阶段 | 有效样本数 | 加权平均 ms |\n|---|---:|---:|\n"
    for tag in ["encode", "backend_txn", "txn", "commit_to_durable", "callback"]:
        valid = [
            m
            for m in minutes
            if m.get("madsys-02_" + tag + "_mean_ms") is not None
            and (m.get("madsys-02_" + tag + "_count") or 0) > 0
        ]
        count = sum(m["madsys-02_" + tag + "_count"] for m in valid)
        mean = (
            sum(
                m["madsys-02_" + tag + "_count"] * m["madsys-02_" + tag + "_mean_ms"]
                for m in valid
            )
            / count
            if count
            else None
        )
        report += (
            "| "
            + tag
            + " | "
            + str(count)
            + " | "
            + (f"{mean:.3f}" if mean is not None else "缺失")
            + " |\n"
        )
    report += "\nTxn 是含编码与后端调用的整体指标；commit_to_durable 包含排队等待，不能把这些嵌套指标直接相加。短测/提前终止窗口应结合 supply_seconds 解释。\n"
    (root / "REPORT.md").write_text(report)
    issue = [
        "# 本轮非预期问题记录",
        "",
        "HA-002 按既定设计记录，不列为待修复问题。以下原因若未有直接证据均保持待确认。",
        "",
    ]
    for i, (title, module, facts, plan) in enumerate(warnings, 1):
        issue += [
            f"## {i}. {title}",
            "",
            f"所属模块：{module}。",
            f'背景：HA+OpLog+Snapshot持续{run["workload"]}供压；参数与报告一致。',
            f"表现及证据：{facts}",
            f"原因及下一步：{plan}",
            "",
        ]
    if not warnings:
        issue += [
            "未从本轮已采集证据发现上述检查范围内的非预期问题；不等于未覆盖的功能全部正确。"
        ]
    (root / "ISSUES.md").write_text("\n".join(issue) + "\n")


if __name__ == "__main__":
    generate(sys.argv[1])
