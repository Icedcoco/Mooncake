"""专用节点执行器：进程身份核验、限量日志、durable 审计和每秒指标。"""

import base64
from collections import Counter
import gzip
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import time
from urllib.request import urlopen
from statistics_report import parse_metrics

ROOT = Path("/opt/mooncake-lab/20261015/results/ha-durable-six-20261109")
LAB = Path("/opt/mooncake-lab/20261015")
IPS = {"madsys-01": "10.1.100.3", "madsys-02": "10.1.100.8", "madsys-03": "10.1.101.3"}
SNAPS = (
    LAB
    / "results/ha-evict-comparison-20261018/shared-snapshots/ha-durable-six-20261109"
)
ETCD = [
    str(LAB / "bin/etcdctl"),
    "--endpoints=http://127.0.0.1:12479",
    "--command-timeout=10s",
]


def save(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False) + "\n")
    tmp.replace(path)


def identity(pid):
    p = Path("/proc") / str(pid)
    stat = (p / "stat").read_text().rsplit(")", 1)[1].split()
    return {
        "pid": pid,
        "start_ticks": stat[19],
        "argv": (p / "cmdline").read_bytes().decode().rstrip("\0").split("\0"),
    }


def alive(rec):
    try:
        cur = identity(rec["pid"])
        return (
            cur == rec
            and (Path("/proc") / str(rec["pid"]) / "stat")
            .read_text()
            .rsplit(")", 1)[1]
            .split()[0]
            != "Z"
        )
    except (OSError, ValueError):
        return False


def terminate(rec, timeout=20):
    if not alive(rec):
        return
    # manifest 必须来自本轮；同时匹配 PID、启动时间和全部 argv，避免 PID 复用。
    assert any(str(ROOT) in a for a in rec["argv"]), rec
    os.kill(rec["pid"], signal.SIGTERM)
    end = time.monotonic() + timeout
    while alive(rec) and time.monotonic() < end:
        time.sleep(0.1)
    if alive(rec):
        os.kill(rec["pid"], signal.SIGKILL)


def register(d, kind, pid):
    # exec 期间 /proc/cmdline 仍可能短暂为空；不得把空 argv 固化为身份。
    deadline = time.monotonic() + 5
    rec = identity(pid)
    start = rec["start_ticks"]
    while not any(rec["argv"]):
        if time.monotonic() >= deadline:
            raise RuntimeError("进程cmdline持续为空：" + str(pid))
        time.sleep(0.01)
        rec = identity(pid)
        if rec["start_ticks"] != start:
            raise RuntimeError("登记时PID已复用：" + str(pid))
    save(d / (kind + ".process.json"), rec)
    return rec


def owned_stop(d, kind):
    path = d / (kind + ".process.json")
    if path.exists():
        terminate(json.loads(path.read_text()))


def managed(d, kind, cmd, env):
    counts = Counter()
    examples = {}
    last = time.monotonic()
    minute = int(time.time() // 60)
    per_min = Counter()
    p = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )
    register(d, kind, p.pid)

    def finish(*_):
        if p.poll() is None:
            p.terminate()

    signal.signal(signal.SIGTERM, finish)
    with gzip.open(d / (kind + "-events.log.gz"), "wt", compresslevel=1) as log, (
        d / (kind + "-warnings.jsonl")
    ).open("a") as warnings:
        for line in p.stdout:
            category = None
            if "PutEnd: OpLog queue failed" in line:
                category = "putend_queue_rejected"
            elif "OpLogApplier: failed to Remove" in line:
                category = "remove_missing_warning"
            elif "BatchEvict: OpLog reservation failed" in line:
                category = "evict_backpressure"
            elif "key not found in etcd" in line:
                category = "control_key_absent"
            if category:
                counts[category] += 1
                per_min[category] += 1
                if per_min[category] == 1:
                    log.write(line)
                examples.setdefault(category, {"first": line[:1500]})["last"] = line[
                    :1500
                ]
            else:
                log.write(line)
            now = time.monotonic()
            if now - last >= 5:
                save(
                    d / (kind + "-log-counts.json"),
                    {"time": time.time(), "counts": counts, "examples": examples},
                )
                if int(time.time() // 60) != minute:
                    warnings.write(
                        json.dumps({"minute_epoch": minute * 60, "counts": per_min})
                        + "\n"
                    )
                    warnings.flush()
                    minute = int(time.time() // 60)
                    per_min.clear()
                log.flush()
                last = now
        warnings.write(
            json.dumps({"minute_epoch": minute * 60, "counts": per_min}) + "\n"
        )
    code = p.wait()
    save(
        d / (kind + "-log-counts.json"),
        {"time": time.time(), "counts": counts, "examples": examples},
    )
    save(d / (kind + "-exit.json"), {"time": time.time(), "rc": code})


def master(d, case, host):
    template = json.loads((ROOT / "master-template.json").read_text())
    args = template["args"]
    old_id = next(a.split("=", 1)[1] for a in args if a.startswith("--cluster_id="))
    args = [
        a.replace(old_id, "mc-oplog-floor-r08-" + case)
        for a in args
        if not a.startswith("--log_dir=")
    ]
    args[0] = str(ROOT / "bin/mooncake_master")
    args = [
        ("--" + a.split("=", 1)[0][2:] + "=" + IPS[host])
        if a.split("=", 1)[0]
        in ("--rpc_address", "--metrics_host", "--http_metadata_server_host")
        else a
        for a in args
    ]
    args += ["--logtostderr=true"]
    env = dict(
        os.environ,
        LD_LIBRARY_PATH=str(ROOT / "lib") + ":" + str(LAB / "deps/aws/lib"),
        MOONCAKE_SNAPSHOT_LOCAL_PATH=str(SNAPS / case),
    )
    save(
        d / "deployment.json",
        {"args": args, "snapshot_path": str(SNAPS / case), "host": host},
    )
    managed(d, "master", args, env)


def client(d):
    env = dict(
        os.environ, LD_LIBRARY_PATH=str(ROOT / "lib") + ":" + str(LAB / "deps/aws/lib")
    )
    managed(
        d,
        "client",
        [str(ROOT / "bin/comparison-pressure"), str(d / "client-config.json")],
        env,
    )


def monitor(d, case, host, duration):
    stopped = False

    def finish(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, finish)
    until = time.monotonic() + duration
    with gzip.open(d / "monitor.jsonl.gz", "wt", compresslevel=1) as f:
        while not stopped and time.monotonic() < until:
            tick = time.monotonic()
            row = {
                "time": time.time(),
                "monotonic": tick,
                "host": host,
                "errors": {},
                "processes": {},
            }
            for name, url in [
                ("metrics", "http://" + IPS[host] + ":49103/metrics"),
                ("health", "http://" + IPS[host] + ":49103/health"),
                ("etcd", "http://127.0.0.1:12479/metrics"),
            ]:
                try:
                    with urlopen(url, timeout=3) as response:
                        value = response.read().decode()
                    row[name] = (
                        json.loads(value) if name == "health" else parse_metrics(value)
                    )
                except Exception as e:
                    row["errors"][name] = str(e)
            for kind in ("master", "client", "etcd"):
                path = d / (kind + ".process.json")
                if not path.exists():
                    continue
                rec = json.loads(path.read_text())
                if not alive(rec):
                    continue
                proc = Path("/proc") / str(rec["pid"])
                try:
                    stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
                    status = (proc / "status").read_text()
                    io = dict(
                        (k, int(v))
                        for k, v in (
                            x.split(":", 1)
                            for x in (proc / "io").read_text().splitlines()
                        )
                    )
                    row["processes"][kind] = {
                        "cpu_seconds": (int(stat[11]) + int(stat[12]))
                        / os.sysconf("SC_CLK_TCK"),
                        "rss_bytes": int(re.search(r"VmRSS:\s+(\d+)", status)[1])
                        * 1024,
                        "io": io,
                    }
                except (OSError, TypeError):
                    pass
            row["available_bytes"] = (
                int(
                    re.search(
                        r"MemAvailable:\s+(\d+)", Path("/proc/meminfo").read_text()
                    )[1]
                )
                * 1024
            )
            row["disk_free_bytes"] = shutil.disk_usage(d).free
            row["io_pressure"] = Path("/proc/pressure/io").read_text()
            row["master_log_counts"] = (
                json.loads((d / "master-log-counts.json").read_text()).get("counts", {})
                if (d / "master-log-counts.json").exists()
                else {}
            )
            row["scrape_seconds"] = time.monotonic() - tick
            guard = []
            if row["available_bytes"] < 6 * 1024**3:
                guard.append("可用内存低于6GiB")
            if row["disk_free_bytes"] < 6 * 1024**3:
                guard.append("磁盘可用低于6GiB")
            if (
                row.get("etcd", {}).get("etcd_mvcc_db_total_size_in_bytes", 0)
                > 18 * 1024**3
            ):
                guard.append("专用etcd DB超过18GiB")
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
            f.flush()
            save(d / "live.json", row)
            if guard:
                save(
                    d / "GUARD.json",
                    {"time": time.time(), "reasons": guard, "row": row},
                )
                owned_stop(d, "client")
                owned_stop(d, "master")
                break
            time.sleep(max(0.05, 1 - (time.monotonic() - tick)))
    if not stopped and time.monotonic() >= until:
        save(
            d / "WATCHDOG.json", {"time": time.time(), "reason": "超过本轮最大运行时间"}
        )
        owned_stop(d, "client")
        owned_stop(d, "master")
        owned_stop(d, "audit")


def audit(d, case):
    prefix = "/oplog/mc-oplog-floor-r08-" + case + "/"
    initial = json.loads(
        subprocess.check_output(
            ETCD + ["get", prefix, "--prefix", "-w", "json"], timeout=15
        )
    )
    for kv in initial.get("kvs", []):
        key = base64.b64decode(kv["key"]).decode()
        assert "/batches/" not in key, "拒绝从非空OpLog开始审计"
    rev = int(initial["header"]["revision"]) + 1
    p = subprocess.Popen(
        ETCD + ["watch", prefix, "--prefix", "--rev=" + str(rev), "-w", "json"],
        stdout=subprocess.PIPE,
        stderr=(d / "audit.stderr").open("w"),
        text=True,
    )
    signal.signal(signal.SIGTERM, lambda *_: p.terminate())
    pending = {}
    batch = seq = deleted = 0
    save(d / "audit-ready.json", {"revision": rev})
    try:
        with gzip.open(d / "audit.jsonl.gz", "wt", compresslevel=1) as out:
            for line in p.stdout:
                obj = json.loads(line)
                if (
                    obj.get("Canceled")
                    or obj.get("CompactRevision")
                    or obj.get("canceled")
                    or obj.get("compact_revision")
                ):
                    raise RuntimeError("审计watch已取消/被compact")
                for event in obj.get("Events", obj.get("events", [])):
                    kv = event["kv"]
                    key = base64.b64decode(kv["key"]).decode()
                    if event.get("type") in ("DELETE", 1):
                        if "/batches/" in key:
                            deleted += 1
                        continue
                    val = base64.b64decode(kv.get("value", ""))
                    if key.startswith(prefix + "batches/"):
                        b = json.loads(val)
                        assert (
                            b[1] == batch + 1
                            and b[2] == seq + 1
                            and b[3] - b[2] + 1 == len(b[4])
                        ), "OpLog序号缺口"
                        batch, seq = b[1], b[3]
                        counts = Counter(str(e[0]) for e in b[4])
                        pending[batch] = dict(
                            batch=batch,
                            first=b[2],
                            last=seq,
                            entries=len(b[4]),
                            counts=counts,
                            bytes=len(val),
                            revision=int(kv["mod_revision"]),
                        )
                    elif key == prefix + "durable_prefix":
                        bound = json.loads(val)
                        r = pending.pop(bound["batch_id"], None)
                        if r is None:
                            if bound["batch_id"] > 0:
                                raise RuntimeError("durable前缀没有对应batch")
                            continue
                        assert r["last"] == bound["last_seq"] and r["revision"] == int(
                            kv["mod_revision"]
                        )
                        r.update(time=time.time(), deleted=deleted)
                        out.write(json.dumps(r, separators=(",", ":")) + "\n")
                        out.flush()
                        save(d / "audit-state.json", r)
        # EOF 必须与控制器发出的停止动作对应；控制器会核对最终前缀。
    except Exception as e:
        save(d / "audit-error.json", {"time": time.time(), "error": repr(e)})
        raise
    finally:
        p.terminate()
        p.wait(timeout=10)


def launch(d, case, kind, host, duration):
    args = [sys.executable, str(ROOT / "node.py"), kind, case, host, str(duration)]
    with (d / (kind + "-supervisor.stdout")).open("ab") as out:
        p = subprocess.Popen(
            args,
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    rec = register(d, kind + "-supervisor", p.pid)
    if kind in ("audit", "monitor"):
        save(d / (kind + ".process.json"), rec)
    print(json.dumps(rec))


if __name__ == "__main__":
    action, case = sys.argv[1:3]
    assert re.fullmatch(r"[A-Za-z0-9_-]+", case)
    d = ROOT / case
    d.mkdir(parents=True, exist_ok=True)
    if action == "launch":
        launch(d, case, sys.argv[3], sys.argv[4], int(sys.argv[5]))
    elif action == "master":
        master(d, case, sys.argv[3])
    elif action == "client":
        client(d)
    elif action == "monitor":
        monitor(d, case, sys.argv[3], int(sys.argv[4]))
    elif action == "audit":
        audit(d, case)
    elif action == "stop":
        owned_stop(d, sys.argv[3])
        # supervisor 在子进程退出后 flush，最多等10秒，不压缩历史原始日志。
        path = d / (sys.argv[3] + "-supervisor.process.json")
        if path.exists():
            rec = json.loads(path.read_text())
            end = time.monotonic() + 10
            while alive(rec) and time.monotonic() < end:
                time.sleep(0.1)
    elif action == "status":
        out = {}
        for name in [
            "live",
            "client-exit",
            "client-log-counts",
            "audit-state",
            "audit-error",
            "GUARD",
            "WATCHDOG",
        ]:
            p = d / (name + ".json")
            if p.exists():
                out[name] = json.loads(p.read_text())
        p = d / "client.jsonl"
        if p.exists():
            with p.open("rb") as f:
                f.seek(max(0, p.stat().st_size - 8192))
                lines = f.read().splitlines()
            try:
                out["client"] = json.loads(lines[-1])
            except (IndexError, ValueError):
                pass
        for kind in ["master", "client", "audit", "monitor"]:
            p = d / (kind + ".process.json")
            if p.exists():
                out[kind + "_alive"] = alive(json.loads(p.read_text()))
        print(json.dumps(out, separators=(",", ":")))
