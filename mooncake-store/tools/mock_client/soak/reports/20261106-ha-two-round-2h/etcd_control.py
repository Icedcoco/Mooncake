"""管理本轮专用etcd；只操作精确的数据目录、token和进程参数。"""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = "/opt/mooncake-lab/20261015"
REMOTE = ROOT + "/results/ha-oplog-floor-20261104-r08"
IPS = {
    "madsys-01": "10.1.100.3",
    "madsys-02": "10.1.100.8",
    "madsys-03": "10.1.101.3",
}
HOSTS = tuple(IPS)
PORT = 12479
PEER_PORT = 12480


def ssh(host, command, timeout=45):
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, command],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(
            host
            + " remote command failed: "
            + result.stderr[-3000:]
            + result.stdout[-1000:]
        )
    return result.stdout.strip()


def write_json(path, value):
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temp.replace(path)


def run_ctl(endpoints, *args, timeout=25):
    command = [ROOT + "/bin/etcdctl", "--endpoints=" + endpoints, *args]
    return ssh("madsys-01", shlex.join(command), timeout=timeout)


def endpoint_list():
    return ",".join("http://" + ip + ":" + str(PORT) for ip in IPS.values())


def wait_health(timeout=60):
    end = time.monotonic() + timeout
    last = ""
    while time.monotonic() < end:
        try:
            rows = json.loads(
                run_ctl(endpoint_list(), "endpoint", "health", "-w", "json", timeout=15)
            )
            if len(rows) == 3 and all(row.get("health") for row in rows):
                return rows
        except Exception as error:
            last = repr(error)
        time.sleep(0.5)
    raise RuntimeError("三成员专用etcd未全部健康：" + last)


def start_cluster(case, compact, leader_host):
    if leader_host not in HOSTS:
        raise ValueError("未知etcd leader主机：" + leader_host)
    state_path = HERE / (case + "-etcd-start.json")
    if state_path.exists():
        raise RuntimeError("拒绝复用etcd场景记录：" + str(state_path))
    token_value = "mc-oplog-floor-20261104-r08-" + case
    quota_bytes = int(os.environ.get("ETCD_QUOTA_BYTES", str(24 * 1024**3)))
    snapshot_count = int(os.environ.get("ETCD_SNAPSHOT_COUNT", "1000"))
    compaction_retention = os.environ.get("ETCD_COMPACTION_RETENTION", "1m")
    compaction_batch_limit = int(os.environ.get("ETCD_COMPACTION_BATCH_LIMIT", "100"))
    compaction_sleep_interval = os.environ.get("ETCD_COMPACTION_SLEEP_INTERVAL", "10ms")
    names = {host: "oplog-floor-r08-" + host for host in HOSTS}
    initial = ",".join(
        names[h] + "=http://" + IPS[h] + ":" + str(PEER_PORT) for h in HOSTS
    )
    result = {
        "case": case,
        "auto_compaction": (
            "periodic-"
            + compaction_retention
            + "-batch"
            + str(compaction_batch_limit)
            + "-sleep"
            + compaction_sleep_interval
            if compact
            else "disabled"
        ),
        "max_request_bytes": os.environ.get(
            "ETCD_MAX_REQUEST_BYTES", "1572864 (default)"
        ),
        "quota_backend_bytes": quota_bytes,
        "snapshot_count": snapshot_count,
        "etcd_leader_host": leader_host,
        "token": token_value,
        "member_processes": {},
        "phase": "starting",
        "started_at": dt.datetime.now().astimezone().isoformat(),
    }
    write_json(state_path, result)
    try:
        for host in HOSTS:
            data = REMOTE + "/etcd/" + case + "/" + host + "/data"
            log = REMOTE + "/etcd/" + case + "/" + host + "/etcd.log"
            args = [
                ROOT + "/bin/etcd",
                "--name=" + names[host],
                "--data-dir=" + data,
                "--listen-client-urls=http://"
                + IPS[host]
                + ":"
                + str(PORT)
                + ",http://127.0.0.1:"
                + str(PORT),
                "--advertise-client-urls=http://" + IPS[host] + ":" + str(PORT),
                "--listen-peer-urls=http://" + IPS[host] + ":" + str(PEER_PORT),
                "--initial-advertise-peer-urls=http://"
                + IPS[host]
                + ":"
                + str(PEER_PORT),
                "--initial-cluster=" + initial,
                "--initial-cluster-state=new",
                "--initial-cluster-token=" + token_value,
                "--quota-backend-bytes=" + str(quota_bytes),
                "--snapshot-count=" + str(snapshot_count),
                "--backend-batch-interval="
                + os.environ.get("ETCD_BACKEND_BATCH_INTERVAL", "100ms"),
                "--backend-batch-limit=10000",
                "--enable-pprof=true",
                "--log-level=info",
            ]
            max_request_bytes = os.environ.get("ETCD_MAX_REQUEST_BYTES")
            if max_request_bytes:
                if int(max_request_bytes) < 1572864:
                    raise ValueError("ETCD_MAX_REQUEST_BYTES不能低于etcd默认值")
                args.append("--max-request-bytes=" + str(int(max_request_bytes)))
            if compact:
                args += [
                    "--auto-compaction-mode=periodic",
                    "--auto-compaction-retention=" + compaction_retention,
                    "--experimental-compaction-batch-limit="
                    + str(compaction_batch_limit),
                    "--experimental-compaction-sleep-interval="
                    + compaction_sleep_interval,
                ]
            payload = json.dumps({"args": args, "log": log, "data": data})
            code = r"""
import json,subprocess
from pathlib import Path
x=json.loads(%s)
data=Path(x["data"])
assert not data.exists(), "拒绝复用etcd数据目录："+str(data)
data.mkdir(parents=True)
log=Path(x["log"]); log.parent.mkdir(parents=True,exist_ok=True)
with log.open("xb") as out:
 p=subprocess.Popen(x["args"],stdin=subprocess.DEVNULL,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
Path(str(data)+".pid").write_text(str(p.pid)+"\n")
print(json.dumps({"pid":p.pid,"args":x["args"],"data":str(data),"log":str(log)}))
""" % repr(payload)
            result["member_processes"][host] = json.loads(
                ssh(host, "python3 -c " + shlex.quote(code), timeout=30)
            )
            write_json(state_path, result)

        health = wait_health(timeout=75)
        statuses = json.loads(
            run_ctl(endpoint_list(), "endpoint", "status", "-w", "json")
        )
        assert len(statuses) == 3
        by_host = {
            row["Endpoint"].split("//", 1)[-1].split(":")[0]: row["Status"]
            for row in statuses
        }
        assert set(by_host) == set(IPS.values()), "etcd endpoint成员不匹配"
        member_id = by_host[IPS[leader_host]]["header"]["member_id"]
        if not all(row["leader"] == member_id for row in by_host.values()):
            # etcdctl move-leader 把 transferee ID 按16进制解析；
            # endpoint status 的 JSON header.member_id 是十进制数值。
            run_ctl(endpoint_list(), "move-leader", format(member_id, "x"), timeout=30)
        for _ in range(40):
            statuses = json.loads(
                run_ctl(endpoint_list(), "endpoint", "status", "-w", "json")
            )
            by_host = {
                row["Endpoint"].split("//", 1)[-1].split(":")[0]: row["Status"]
                for row in statuses
            }
            if all(row["leader"] == member_id for row in by_host.values()):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("无法将etcd leader迁移到 " + leader_host)
        pprof = {}
        for host in HOSTS:
            code = (
                r"""
from urllib.request import urlopen
r=urlopen("http://127.0.0.1:%d/debug/pprof/",timeout=3)
assert r.status==200
print(r.status)
"""
                % PORT
            )
            pprof[host] = ssh(host, "python3 -c " + shlex.quote(code))
        result.update(
            phase="ready",
            etcd_leader_member_id=member_id,
            endpoint_health=health,
            endpoint_status=statuses,
            pprof_http=pprof,
            ready_at=dt.datetime.now().astimezone().isoformat(),
        )
        write_json(state_path, result)
        return result
    except BaseException:
        # 部分启动也按已记录的精确PID/token/data目录停止，保留所有数据文件。
        try:
            stop_cluster(case, result)
        except Exception as cleanup_error:
            result["cleanup_error"] = repr(cleanup_error)
            write_json(state_path, result)
        raise


def stop_cluster(case, start_record=None):
    if start_record is None:
        start_record = json.loads((HERE / (case + "-etcd-start.json")).read_text())
    token = "--initial-cluster-token=" + start_record["token"]
    stopped = {}
    for host, proc in start_record["member_processes"].items():
        pid = proc["pid"]
        data = proc["data"]
        code = r"""
import json,os,signal,time
from pathlib import Path
pid=int(%d); token=%r; data=%r; binary=%r
p=Path("/proc")/str(pid)
if p.exists():
 argv=[x for x in (p/"cmdline").read_bytes().decode().split(chr(0)) if x]
 assert argv and argv[0]==binary and token in argv and "--data-dir="+data in argv, "拒绝停止身份不匹配的进程"
 os.kill(pid,signal.SIGTERM)
 for _ in range(200):
  try:
   if (Path("/proc")/str(pid)/"stat").read_text().rsplit(")",1)[1].split()[0]=="Z": break
  except FileNotFoundError: break
  time.sleep(.1)
 else: raise RuntimeError("etcd未按时停止")
print(json.dumps({"pid":pid,"stopped":True}))
""" % (pid, token, data, ROOT + "/bin/etcd")
        stopped[host] = json.loads(
            ssh(host, "python3 -c " + shlex.quote(code), timeout=35)
        )

    logs = {}
    for host, proc in start_record["member_processes"].items():
        local = HERE / "etcd-logs" / case / (host + ".log")
        local.parent.mkdir(parents=True, exist_ok=True)
        remote_log = proc["log"]
        subprocess.run(["scp", "-q", host + ":" + remote_log, str(local)], check=True)
        logs[host] = {
            "path": str(local.relative_to(HERE)),
            "bytes": local.stat().st_size,
            "sha256": hashlib.sha256(local.read_bytes()).hexdigest(),
        }
    result = {
        "case": case,
        "stopped": stopped,
        "logs": logs,
        "data_dirs_preserved": True,
        "finished_at": dt.datetime.now().astimezone().isoformat(),
    }
    write_json(HERE / (case + "-etcd-stop.json"), result)
    return result


if __name__ == "__main__":
    import sys

    action = sys.argv[1]
    if action == "start":
        row = start_cluster(sys.argv[2], sys.argv[3] == "on", sys.argv[4])
        print(json.dumps(row, ensure_ascii=False))
    elif action == "stop":
        row = json.loads((HERE / (sys.argv[2] + "-etcd-start.json")).read_text())
        print(json.dumps(stop_cluster(sys.argv[2], row), ensure_ascii=False))
    else:
        raise SystemExit("用法：etcd_control.py start CASE on|off LEADER|stop CASE")
