"""仅在 madsys-01 编译供压工具；master 和依赖直接复用已验证 SHA256。"""

import hashlib
import json
from pathlib import Path
import shlex
import subprocess

HERE = Path(__file__).resolve().parent
REMOTE = "/opt/mooncake-lab/20261015/results/ha-two-round-20261106"
OLD = "/opt/mooncake-lab/20261015/results/compaction-isolation-20261105-r09/master"
HOSTS = ("madsys-01", "madsys-02", "madsys-03")


def ssh(h, code, timeout=30):
    return subprocess.check_output(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", h, code],
        text=True,
        timeout=timeout,
    )


def main():
    expected = json.loads(
        (HERE.parent / "20261105-compaction-isolation/execution-r10m3.json").read_text()
    )["deployment_checks"]["madsys-01"]["hashes"]
    records = {}
    for h in HOSTS:
        code = """from pathlib import Path
import json,hashlib,shutil,subprocess,re
p=Path(%r); old=Path(%r)
assert not [x for x in subprocess.check_output(['ss','-ltnp'],text=True).splitlines() if re.search(r':(?:12479|12480|45151|49103|48180)\\b',x)]
assert shutil.disk_usage(p.parent).free > 24*1024**3
for sub in ['bin','lib']: (p/sub).mkdir(parents=True,exist_ok=True)
for rel in ['bin/mooncake_master','lib/libasio.so','lib/libetcd_wrapper.so']: shutil.copy2(old/rel,p/rel)
print(json.dumps({rel:hashlib.sha256((p/rel).read_bytes()).hexdigest() for rel in ['bin/mooncake_master','lib/libasio.so','lib/libetcd_wrapper.so']}))
""" % (REMOTE, OLD)
        hashes = json.loads(ssh(h, "python3 -c " + shlex.quote(code)))
        assert hashes["bin/mooncake_master"] == expected["mooncake_master"]
        assert hashes["lib/libasio.so"] == expected["libasio.so"]
        assert (
            hashes["lib/libetcd_wrapper.so"]
            == "9948af4c27e355fbf625a373bdfdbddefdf3a3cd999429abf4b7889f4aa83ea4"
        )
        records[h] = hashes
    for name in ["pressure.cpp", "window.h", "window_test.cpp"]:
        subprocess.run(
            ["scp", "-q", str(HERE / name), "madsys-01:" + REMOTE + "/" + name],
            check=True,
        )
    spec = json.loads(
        (HERE.parent / "20261018-ha-evict-comparison/build-command.json").read_text()
    )
    root = "/home/kyc/orca/workspaces/Mooncake/ha-lab-preserve-202610"
    build = "/opt/mooncake-lab/20261015/build-ha-oplog-longtail-r07"
    source = "/opt/mooncake-lab/20261015/src-ha-oplog-longtail-r07"

    def replace(v):
        old = (
            root
            + "/mooncake-store/tools/mock_client/soak/reports/20261018-ha-evict-comparison/pressure"
        )
        if v in (old, old + ".cpp", old + ".o"):
            return REMOTE + (
                "/bin/comparison-pressure" if v == old else "/pressure" + v[len(old) :]
            )
        return (
            v.replace(
                root + "/build/_deps/yalantinglibs-src",
                "/opt/mooncake-lab/20261015/deps/fetch/yalantinglibs-src",
            )
            .replace(root + "/build", build)
            .replace(root, source)
        )

    commands = [([replace(x) for x in cmd], replace(cwd)) for cmd, cwd in spec]
    (HERE / "build-command.json").write_text(json.dumps(commands, indent=2) + "\n")
    ssh(
        "madsys-01",
        "cd "
        + shlex.quote(REMOTE)
        + " && g++ -std=c++17 window_test.cpp -o window-test && ./window-test",
    )
    for cmd, cwd in commands:
        print("在 madsys-01 串行编译/链接", cmd[0], flush=True)
        ssh(
            "madsys-01",
            "cd " + shlex.quote(cwd) + " && " + shlex.join(cmd),
            timeout=300,
        )
    subprocess.run(
        [
            "scp",
            "-q",
            "madsys-01:" + REMOTE + "/bin/comparison-pressure",
            str(HERE / "comparison-pressure"),
        ],
        check=True,
    )
    binary = HERE / "comparison-pressure"
    sha = hashlib.sha256(binary.read_bytes()).hexdigest()
    subprocess.run(
        ["scp", "-q", str(binary), "madsys-03:" + REMOTE + "/bin/comparison-pressure"],
        check=True,
    )
    assert (
        ssh("madsys-03", "sha256sum " + REMOTE + "/bin/comparison-pressure").split()[0]
        == sha
    )
    records["pressure_sha256"] = sha
    records["pressure_source_sha256"] = hashlib.sha256(
        (HERE / "pressure.cpp").read_bytes()
    ).hexdigest()
    records["master_source_commit"] = "8aac9fd427933a0809edc48f032329cd89588eef"
    (HERE / "build-manifest.json").write_text(json.dumps(records, indent=2) + "\n")
    print("构建和二进制校验完成", sha, flush=True)


if __name__ == "__main__":
    main()
