"""两轮串行、断开终端后仍运行；停止与清理均绑定专用身份。"""

import argparse
import base64
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tarfile
import time
import traceback
from statistics_report import generate

HERE = Path(__file__).resolve().parent
REMOTE = "/opt/mooncake-lab/20261015/results/ha-two-round-20261106"
HOSTS = ("madsys-01", "madsys-02", "madsys-03")
sys.path.insert(0, str(HERE.parent / "20261104-ha-oplog-floor-serializable/r08"))
import etcd_control as ctl  # noqa: E402 - 显式加载已验证的专用集群部署工具。

sys.path.insert(0, str(HERE.parent / "20261105-compaction-isolation"))
from lifecycle import stop_cluster  # noqa: E402

ctl.REMOTE = REMOTE
cancelled = False


def save(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def ssh(h, args, timeout=30):
    return ctl.ssh(h, shlex.join(args), timeout=timeout)


def code(h, text, timeout=30):
    return ssh(h, ["python3", "-c", text], timeout)


def node(h, action, case, *args, timeout=30):
    return ssh(
        h, ["python3", REMOTE + "/node.py", action, case, *map(str, args)], timeout
    )


def wait_for(fn, seconds=90):
    until = time.monotonic() + seconds
    error = ""
    while time.monotonic() < until:
        try:
            value = fn()
            if value:
                return value
        except Exception as e:
            error = repr(e)
        if cancelled:
            raise RuntimeError("用户停止测试")
        time.sleep(1)
    raise RuntimeError("等待超时：" + error)


def deploy_scripts():
    template = next(
        (HERE.parent / "20261105-compaction-isolation").glob(
            "D-w2-*-r10m3/madsys-02/deployment.json"
        )
    )
    for h in HOSTS:
        for src in [HERE / "node.py", HERE / "statistics_report.py", template]:
            name = "master-template.json" if src == template else src.name
            subprocess.run(
                ["scp", "-q", str(src), h + ":" + REMOTE + "/" + name],
                check=True,
                timeout=30,
            )
    # 验证三个 master 看到同一个 Snapshot 目录，而非各自的同名本地目录。
    marker = "snapshot-share-" + str(os.getpid()) + "-" + str(time.time_ns())
    code(
        "madsys-01",
        "import sys;sys.path.insert(0,%r);from node import SNAPS;SNAPS.mkdir(parents=True,exist_ok=True);(SNAPS/%r).write_text(%r)"
        % (REMOTE, marker, marker),
    )
    try:
        for h in HOSTS:
            code(
                h,
                "import sys;sys.path.insert(0,%r);from node import SNAPS;assert (SNAPS/%r).read_text()==%r"
                % (REMOTE, marker, marker),
            )
    finally:
        code(
            "madsys-01",
            "import sys;sys.path.insert(0,%r);from node import SNAPS;(SNAPS/%r).unlink()"
            % (REMOTE, marker),
        )


def statuses(case):
    out = {}
    # 只并行读状态；部署/编译/供压均串行。
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(node, h, "status", case): h for h in HOSTS}
        for task, h in futures.items():
            try:
                out[h] = json.loads(task.result())
            except Exception as e:
                out[h] = {"ssh_error": repr(e)}
    return out


def check_stopped(case, state):
    for h in HOSTS:
        result = code(
            h,
            """from pathlib import Path
import json,sys
sys.path.insert(0,%r)
from node import alive
d=Path(%r)
active=[]
for p in d.glob('*.process.json'):
 r=json.loads(p.read_text())
 if alive(r): active.append({'manifest':p.name,'pid':r['pid'],'argv':r['argv']})
print(json.dumps(active))
"""
            % (REMOTE, REMOTE + "/" + case),
        )
        if json.loads(result):
            raise RuntimeError("本轮仍有进程，禁止进入下一轮：" + h + result)
    if state and state.get("pending"):
        raise RuntimeError("etcd未完全停止")


def collect(case, dest):
    for h in HOSTS:
        folder = dest / h
        folder.mkdir(exist_ok=True)
        archive = dest / (h + ".tar")
        with archive.open("wb") as out:
            subprocess.run(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "ConnectTimeout=8",
                    h,
                    shlex.join(["tar", "-C", REMOTE + "/" + case, "-cf", "-", "."]),
                ],
                stdout=out,
                check=True,
                timeout=180,
            )
        with tarfile.open(archive) as tf:
            for m in tf.getmembers():
                assert (
                    not m.issym()
                    and not m.islnk()
                    and (folder / m.name).resolve().is_relative_to(folder.resolve())
                )
            tf.extractall(folder, filter="data")
        archive.unlink()


def final_boundary(case):
    raw = json.loads(
        ctl.run_ctl(
            ctl.endpoint_list(),
            "get",
            "/oplog/mc-oplog-floor-r08-" + case + "/durable_prefix",
            "-w",
            "json",
        )
    )
    if not raw.get("kvs"):
        return None
    return json.loads(base64.b64decode(raw["kvs"][0]["value"]))


def run_case(workload, seconds, label):
    case = label + "-" + workload.replace("_", "-")
    dest = HERE / case
    dest.mkdir(exist_ok=False)
    ctl.HERE = dest
    state = None
    started = []
    monitors = []
    audit = False
    client = False
    run = {
        "case": case,
        "workload": workload,
        "duration": seconds,
        "phase": "preparing",
        "started": time.time(),
        "client_rc": None,
    }
    save(dest / "run.json", run)
    try:
        for h in HOSTS:
            check = code(
                h,
                """from pathlib import Path
import subprocess,re,shutil,json
d=Path(%r)
assert not d.exists(), '场景目录已存在'
assert not [x for x in subprocess.check_output(['ss','-ltnp'],text=True).splitlines() if re.search(r':(?:12479|12480|45151|49103|48180)\\b',x)], '专用端口被占用'
assert shutil.disk_usage(d.parent).free>24*1024**3, '磁盘余量不足24GiB'
d.mkdir()
print(json.dumps({'time':__import__('time').time(),'free_bytes':shutil.disk_usage(d).free}))
"""
                % (REMOTE + "/" + case),
            )
            run.setdefault("preflight", {})[h] = json.loads(check)
        os.environ.update(
            ETCD_QUOTA_BYTES=str(24 * 1024**3),
            ETCD_MAX_REQUEST_BYTES=str(8 * 1024**2),
            ETCD_BACKEND_BATCH_INTERVAL="100ms",
            ETCD_COMPACTION_RETENTION="5m",
            ETCD_COMPACTION_BATCH_LIMIT="8",
            ETCD_COMPACTION_SLEEP_INTERVAL="100ms",
            ETCD_SNAPSHOT_COUNT="1000",
        )
        state = ctl.start_cluster(case, True, "madsys-01")
        run["etcd_start"] = state
        save(dest / "run.json", run)
        for h in HOSTS:
            pid = state["member_processes"][h]["pid"]
            code(
                h,
                "import sys;sys.path.insert(0,%r);from node import register;from pathlib import Path;register(Path(%r),'etcd',%d)"
                % (REMOTE, REMOTE + "/" + case, pid),
            )
        for h in ["madsys-02", "madsys-01", "madsys-03"]:
            node(h, "launch", case, "master", h, seconds)
            started.append(h)

            def ready(host=h):
                text = code(
                    host,
                    "from urllib.request import urlopen;print(urlopen('http://%s:49103/health',timeout=3).read().decode())"
                    % ctl.IPS[host],
                )
                health = json.loads(text)
                expected = "leader" if host == "madsys-02" else "standby"
                return (
                    health
                    if health.get("role") == expected
                    and (expected != "leader" or health.get("service_ready"))
                    else None
                )

            wait_for(ready)
        for h in HOSTS:
            node(h, "launch", case, "monitor", h, seconds + 900)
            monitors.append(h)
        node("madsys-01", "launch", case, "audit", "madsys-01", seconds + 900)
        audit = True
        wait_for(
            lambda: code(
                "madsys-01",
                "from pathlib import Path;print((Path(%r)/'audit-ready.json').exists())"
                % (REMOTE + "/" + case),
            ).strip()
            == "True"
        )
        # key 和 segment 名称都进入 OpLog，固定为上一轮 r10m3 的82字节。
        assert len(case.encode("ascii")) <= 82
        config = {
            "endpoint": "10.1.100.8:45151",
            "run_id": case.ljust(82, "x"),
            "workers": 2,
            "batch_size": 128,
            "duration_seconds": seconds,
            "verify_every_batches": 16,
            "capacity_bytes": 256 * 1024**2,
            "object_bytes": 1024,
            "seed": int(time.time()),
            "output": REMOTE + "/" + case + "/client.jsonl",
            "hold_seconds": 180,
            "workload": workload,
            "live_target_keys": 65536,
        }
        save(dest / "client-config.json", config)
        subprocess.run(
            [
                "scp",
                "-q",
                str(dest / "client-config.json"),
                "madsys-03:" + REMOTE + "/" + case + "/client-config.json",
            ],
            check=True,
            timeout=30,
        )
        node("madsys-03", "launch", case, "client", "madsys-03", seconds)
        client = True
        run.update(phase="running", workload_launched=time.time())
        save(dest / "run.json", run)
        last_healthy = {h: time.monotonic() for h in HOSTS}
        deadline = time.monotonic() + seconds + 300
        with (dest / "controller-status.jsonl").open("a") as timeline:
            while time.monotonic() < deadline:
                if cancelled:
                    raise RuntimeError("用户停止测试")
                s = statuses(case)
                save(dest / "live-status.json", s)
                summary = {"time": time.time(), "nodes": {}}
                for h, v in s.items():
                    if "ssh_error" in v:
                        if time.monotonic() - last_healthy[h] > 600:
                            raise RuntimeError(h + "连接中断超过10分钟")
                        summary["nodes"][h] = {"ssh_error": v["ssh_error"]}
                        continue
                    last_healthy[h] = time.monotonic()
                    if v.get("GUARD") or v.get("WATCHDOG") or v.get("audit-error"):
                        raise RuntimeError(
                            h
                            + "触发保护或审计失败："
                            + json.dumps(
                                {
                                    k: v[k]
                                    for k in ["GUARD", "WATCHDOG", "audit-error"]
                                    if k in v
                                },
                                ensure_ascii=False,
                            )[:800]
                        )
                    if not v.get("master_alive"):
                        raise RuntimeError(h + " master进程已退出")
                    if not v.get("monitor_alive"):
                        raise RuntimeError(h + "指标采集进程已退出")
                    live = v.get("live", {})
                    metrics = live.get("metrics", {})
                    health = live.get("health", {})
                    if time.time() - live.get("time", 0) > 60:
                        raise RuntimeError(h + "指标采集超过60秒无进展")
                    if health and health.get("role") != (
                        "leader" if h == "madsys-02" else "standby"
                    ):
                        raise RuntimeError(
                            "高压稳态测试发生master角色变化：" + h + repr(health)
                        )
                    if h == "madsys-01" and not v.get("audit_alive"):
                        raise RuntimeError("durable审计进程提前结束")
                    summary["nodes"][h] = {
                        "health": health,
                        "durable_seq": metrics.get("ha_batch_record_durable_sequence"),
                        "applied_seq": metrics.get("ha_oplog_applied_sequence_id"),
                        "available_bytes": live.get("available_bytes"),
                        "db_bytes": live.get("etcd", {}).get(
                            "etcd_mvcc_db_total_size_in_bytes"
                        ),
                        "snapshot_latest": metrics.get("ha_snapshot_latest_batch"),
                        "floor": metrics.get("ha_snapshot_compaction_floor"),
                    }
                c = s.get("madsys-03", {}).get("client", {})
                summary["client"] = c
                timeline.write(json.dumps(summary, ensure_ascii=False) + "\n")
                timeline.flush()
                print(
                    case,
                    round(c.get("elapsed", 0)),
                    "成功",
                    c.get("success"),
                    "remove",
                    c.get("remove_success"),
                    "durable",
                    summary["nodes"].get("madsys-02", {}).get("durable_seq"),
                    flush=True,
                )
                if c.get("phase") == "done":
                    run["client_done"] = c
                    save(dest / "run.json", run)
                    if c.get("elapsed", 0) < seconds - 1:
                        raise RuntimeError("client提前停止")
                    break
                if s.get("madsys-03", {}).get("client-exit"):
                    raise RuntimeError("client退出但没有正常done记录")
                time.sleep(15)
            else:
                raise RuntimeError("client超过截止时间")
        # client继续Ping，最多等待90秒追平，避免先关standby导致收尾边界偏差。
        settle_end = time.monotonic() + 90
        stable = 0
        previous = None
        while time.monotonic() < settle_end:
            boundary = final_boundary(case)
            s = statuses(case)
            target = boundary["last_seq"]
            caught = all(
                v.get("live", {})
                .get("metrics", {})
                .get("ha_oplog_applied_sequence_id", -1)
                >= target
                for h, v in s.items()
                if h != "madsys-02"
            )
            stable = stable + 1 if previous == target and caught else 0
            previous = target
            if stable >= 2:
                break
            time.sleep(5)
        run["settle_boundary"] = boundary
        run["standby_caught_up"] = bool(stable >= 2)
        run["phase"] = "supply_complete"
    except BaseException as e:
        run.update(phase="failed", error=repr(e), traceback=traceback.format_exc())
        print(case, "异常", repr(e), flush=True)
    finally:
        save(dest / "run.json", run)
        failures = []
        if client:
            try:
                node("madsys-03", "stop", case, "client")
            except Exception as e:
                failures.append(repr(e))
        # 先停两个standby再停leader，停止动作快速且不阻塞于压缩大日志。
        for h in ["madsys-03", "madsys-01", "madsys-02"]:
            if h in started:
                try:
                    node(h, "stop", case, "master", timeout=45)
                except Exception as e:
                    failures.append(repr(e))
        if state:
            try:
                run["final_boundary"] = final_boundary(case)
                save(
                    dest / "etcd-final-status.json",
                    json.loads(
                        ctl.run_ctl(
                            ctl.endpoint_list(), "endpoint", "status", "-w", "json"
                        )
                    ),
                )
                if audit and run["final_boundary"]:
                    wait_for(
                        lambda: json.loads(node("madsys-01", "status", case))
                        .get("audit-state", {})
                        .get("last")
                        == run["final_boundary"]["last_seq"],
                        60,
                    )
            except Exception as e:
                failures.append("收尾边界：" + repr(e))
        if audit:
            try:
                node("madsys-01", "stop", case, "audit")
            except Exception as e:
                failures.append(repr(e))
        for h in monitors:
            try:
                node(h, "stop", case, "monitor")
            except Exception as e:
                failures.append(repr(e))
        try:
            collect(case, dest)
            p = dest / "madsys-03/client-exit.json"
            if p.exists():
                run["client_rc"] = json.loads(p.read_text())["rc"]
        except Exception as e:
            failures.append("归档：" + repr(e))
        stopped = None
        if state:
            try:
                stopped = stop_cluster(ctl, case, state)
                run["etcd_stop"] = stopped
            except Exception as e:
                failures.append("etcd停止：" + repr(e))
        try:
            check_stopped(case, stopped)
            if state and stopped and not stopped.get("pending"):
                for h, p in state["member_processes"].items():
                    code(
                        h,
                        "from pathlib import Path;import shutil;p=Path(%r);assert p==Path(%r)/'data';shutil.rmtree(p)"
                        % (p["data"], REMOTE + "/etcd/" + case + "/" + h),
                    )
                run["data_dirs_removed_after_stop"] = True
        except Exception as e:
            failures.append("清理核验：" + repr(e))
        run["cleanup_errors"] = failures
        run["finished"] = time.time()
        if (
            run["phase"] == "supply_complete"
            and run.get("client_rc") == 0
            and not failures
        ):
            run["phase"] = "complete"
        elif run["phase"] != "failed":
            run["phase"] = "incomplete"
        save(dest / "run.json", run)
        shutil_manifest = HERE / "build-manifest.json"
        (dest / "build-manifest.json").write_bytes(shutil_manifest.read_bytes())
        try:
            generate(dest)
        except Exception as e:
            run["report_error"] = repr(e)
            if run["phase"] == "complete":
                run["phase"] = "incomplete"
            save(dest / "run.json", run)
        print("本轮结束", case, run["phase"], flush=True)
    if failures:
        raise RuntimeError(
            "上一轮停止/清理未完全核验，禁止启动下一轮：" + repr(failures)
        )
    return run


def main():
    global cancelled
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=7200)
    p.add_argument("--label", required=True)
    p.add_argument(
        "--workloads",
        nargs="+",
        default=["put_evict", "put_remove"],
        choices=["put_evict", "put_remove"],
    )
    args = p.parse_args()
    assert args.seconds > 0
    lock = (HERE / "suite.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def stop(*_):
        global cancelled
        cancelled = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    deploy_scripts()
    suite = {
        "label": args.label,
        "duration_per_round": args.seconds,
        "workloads": args.workloads,
        "phase": "running",
        "pid": os.getpid(),
        "started": time.time(),
        "rounds": [],
    }
    save(HERE / (args.label + "-suite.json"), suite)
    try:
        for workload in args.workloads:
            if cancelled:
                break
            result = run_case(workload, args.seconds, args.label)
            suite["rounds"].append(
                {
                    "case": result["case"],
                    "phase": result["phase"],
                    "report": str(HERE / result["case"] / "REPORT.md"),
                }
            )
            save(HERE / (args.label + "-suite.json"), suite)
        suite["phase"] = (
            "complete"
            if len(suite["rounds"]) == len(args.workloads)
            and all(r["phase"] == "complete" for r in suite["rounds"])
            else "incomplete"
        )
    except BaseException as e:
        suite.update(phase="failed", error=repr(e))
        raise
    finally:
        suite["finished"] = time.time()
        save(HERE / (args.label + "-suite.json"), suite)


if __name__ == "__main__":
    main()
