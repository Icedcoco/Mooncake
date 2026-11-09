"""专用集群停止：验证完整身份，容忍 compaction 收尾，不强杀其他进程。"""

import datetime
import hashlib
import shlex
import subprocess
import time


def stop_cluster(ctl, case, start_record):
    stopped = {}
    pending = dict(start_record["member_processes"])
    began = time.monotonic()
    for host, proc in pending.items():
        code = """
import os,signal,json
from pathlib import Path
p=Path('/proc')/str(%r)
if p.exists() and (p/'stat').read_text().rsplit(')',1)[1].split()[0]!='Z':
 a=(p/'cmdline').read_bytes().decode().split(chr(0))
 assert a[0]==%r and %r in a and %r in a, '拒绝停止身份不匹配的进程'
 os.kill(int(p.name),signal.SIGTERM)
""" % (
            proc["pid"],
            ctl.ROOT + "/bin/etcd",
            "--initial-cluster-token=" + start_record["token"],
            "--data-dir=" + proc["data"],
        )
        ctl.ssh(host, "python3 -c " + shlex.quote(code))
    while pending and time.monotonic() - began < 90:
        for host, proc in list(pending.items()):
            code = (
                "from pathlib import Path;p=Path('/proc')/str(%r);print(not p.exists() or (p/'stat').read_text().rsplit(')',1)[1].split()[0]=='Z')"
                % proc["pid"]
            )
            if ctl.ssh(host, "python3 -c " + shlex.quote(code)) == "True":
                stopped[host] = {
                    "pid": proc["pid"],
                    "stopped": True,
                    "seconds": time.monotonic() - began,
                }
                del pending[host]
        if pending:
            time.sleep(2)
    logs = {}
    for host, proc in start_record["member_processes"].items():
        local = ctl.HERE / "etcd-logs" / case / (host + ".log")
        local.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["scp", "-q", host + ":" + proc["log"], str(local)], check=True)
        logs[host] = {
            "path": str(local.relative_to(ctl.HERE)),
            "bytes": local.stat().st_size,
            "sha256": hashlib.sha256(local.read_bytes()).hexdigest(),
        }
    result = {
        "case": case,
        "stopped": stopped,
        "pending": pending,
        "logs": logs,
        "data_dirs_preserved": True,
        "finished_at": datetime.datetime.now().astimezone().isoformat(),
    }
    ctl.write_json(ctl.HERE / (case + "-etcd-stop.json"), result)
    if pending:
        raise RuntimeError("停止未完成，保留本轮数据：" + repr(list(pending)))
    return result
