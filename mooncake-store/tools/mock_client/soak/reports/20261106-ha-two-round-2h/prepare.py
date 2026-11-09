"""在 madsys-01 单线程构建本分支，测试后向三台专用测试节点部署。"""

import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

CODE_DIR = Path(__file__).resolve().parent
REPO = next(
    p
    for p in CODE_DIR.parents
    if (p / "CMakeLists.txt").exists() and (p / ".git").exists()
)
HERE = Path(os.environ.get("MOONCAKE_PRESSURE_RESULTS", str(CODE_DIR)))
LAB = "/opt/mooncake-lab/20261015"
REMOTE = LAB + "/results/ha-durable-six-20261109"
SOURCE = LAB + "/src-ha-durable-six-20261109-r02"
BUILD = LAB + "/build-ha-durable-six-20261109-r02"
HOSTS = ("madsys-01", "madsys-02", "madsys-03")
GO = "/root/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.10.linux-amd64/bin"


def ssh(host, command, timeout=60):
    return subprocess.check_output(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", host, command],
        text=True,
        timeout=timeout,
    )


def remote_code(host, code, timeout=60):
    return ssh(host, "python3 -c " + shlex.quote(code), timeout)


def main():
    HERE.mkdir(parents=True, exist_ok=True)
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
    ).strip()
    assert not subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=REPO
    ).strip(), "必须先提交代码再构建"
    for host in HOSTS:
        print(
            host,
            remote_code(
                host,
                f"""from pathlib import Path
import shutil,subprocess,re,json
p=Path({REMOTE!r}); p.mkdir(parents=True,exist_ok=True)
assert not [x for x in subprocess.check_output(['ss','-ltnp'],text=True).splitlines() if re.search(r':(?:12479|12480|45151|49103|48180)\\b',x)], '测试端口仍被占用'
assert shutil.disk_usage(p).free>24*1024**3, '磁盘余量不足24GiB'
for name in ['bin','lib']: (p/name).mkdir(exist_ok=True)
print(json.dumps({{'free_bytes':shutil.disk_usage(p).free}}))
""",
            ),
            flush=True,
        )
    archive = HERE / "source.tar.gz"
    subprocess.run(
        ["git", "archive", "--format=tar.gz", "-o", str(archive), commit],
        cwd=REPO,
        check=True,
    )
    source_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    subprocess.run(
        ["scp", "-q", str(archive), "madsys-01:" + REMOTE + "/source.tar.gz"],
        check=True,
    )
    print(
        remote_code(
            "madsys-01",
            f"""from pathlib import Path
import hashlib,json,tarfile,shutil
root=Path({SOURCE!r}); commit={commit!r}; arc=Path({REMOTE!r})/'source.tar.gz'
assert hashlib.sha256(arc.read_bytes()).hexdigest()=={source_sha!r}
if root.exists(): assert (root/'BUILD-SOURCE.json').exists() and json.loads((root/'BUILD-SOURCE.json').read_text())['commit']==commit
else:
 root.mkdir()
 with tarfile.open(arc) as tf: tf.extractall(root)
 (root/'BUILD-SOURCE.json').write_text(json.dumps({{'commit':commit,'archive_sha256':{source_sha!r}}}))
 p=root/'extern/pybind11'
 if not (p/'CMakeLists.txt').exists(): shutil.copytree({LAB!r}+'/deps/pybind11',p,dirs_exist_ok=True)
print('源码已展开：'+commit)
""",
        ),
        flush=True,
    )
    dependency_commit = "54f9c0642386e7860e8e11ada698c484dc562d7d"
    dependency_sha = "11040b1d3f859599e5453ef7e9b5f84f8afa2388b057330f4c667973e5b20f01"
    dependency_archive = HERE / "yalantinglibs-54f9c064.tar.gz"
    assert dependency_commit in (REPO / "mooncake-common/FindYLT.cmake").read_text()
    if not dependency_archive.exists():
        subprocess.run(
            [
                "curl",
                "-sSL",
                "--fail",
                "--max-time",
                "300",
                "https://codeload.github.com/alibaba/yalantinglibs/tar.gz/"
                + dependency_commit,
                "-o",
                str(dependency_archive),
            ],
            check=True,
        )
    assert hashlib.sha256(dependency_archive.read_bytes()).hexdigest() == dependency_sha
    subprocess.run(
        [
            "scp",
            "-q",
            str(dependency_archive),
            "madsys-01:" + REMOTE + "/yalantinglibs.tar.gz",
        ],
        check=True,
    )
    dependency_source = LAB + "/deps/fetch/yalantinglibs-" + dependency_commit
    print(
        remote_code(
            "madsys-01",
            f"""from pathlib import Path
import tarfile,hashlib
arc=Path({REMOTE!r})/'yalantinglibs.tar.gz'
assert hashlib.sha256(arc.read_bytes()).hexdigest()=={dependency_sha!r}
d=Path({dependency_source!r})
if not d.exists():
 with tarfile.open(arc) as tf: tf.extractall(d.parent)
print('依赖校验通过：'+str(d))
""",
        ),
        flush=True,
    )
    configure = [
        "cmake",
        "-S",
        SOURCE,
        "-B",
        BUILD,
        "-DCMAKE_BUILD_TYPE=RelWithDebInfo",
        "-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=-O2 -g1 -DNDEBUG",
        "-DWITH_STORE=ON",
        "-DWITH_TE=ON",
        "-DWITH_METRICS=ON",
        "-DBUILD_MOCK_CLIENT=ON",
        "-DBUILD_UNIT_TESTS=ON",
        "-DBUILD_EXAMPLES=OFF",
        "-DBUILD_BENCHMARK=OFF",
        "-DSTORE_USE_ETCD=ON",
        "-DSTORE_USE_REDIS=OFF",
        "-DSTORE_USE_K8S_LEASE=OFF",
        "-DSTORE_USE_JEMALLOC=OFF",
        "-DUSE_CUDA=OFF",
        "-DUSE_TENT=OFF",
        "-DUSE_NOF=OFF",
        "-DUSE_NVMEOF=OFF",
        "-DUSE_TCP=ON",
        "-DUSE_HTTP=ON",
        "-DWITH_STORE_RUST=OFF",
        "-DWITH_STORE_GO=OFF",
        "-DWITH_STORE_C_SHARED=OFF",
        "-DWITH_EP=OFF",
        "-DMOONCAKE_ENABLE_OPLOG_PERF_METRICS=ON",
        "-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=" + dependency_source,
        "-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=" + LAB + "/deps/fetch/googletest-src",
        "-DCMAKE_PREFIX_PATH=" + LAB + "/deps/aws",
    ]
    targets = [
        "mooncake_master",
        "mooncake_master_mock_client",
        "mooncake_durable_pressure",
        "mock_client_core_test",
        "mock_client_live_window_test",
        "oplog_batch_standby_reader_test",
        "batch_oplog_pruning_worker_test",
        "master_service_config_test",
    ]
    commands = [
        configure,
        ["cmake", "--build", BUILD, "--parallel", "1", "--target", *targets],
        [
            "ctest",
            "--test-dir",
            BUILD,
            "--output-on-failure",
            "-j1",
            "-R",
            "^(mock_client_core_test|mock_client_live_window_test|oplog_batch_standby_reader_test|batch_oplog_pruning_worker_test|master_service_config_test|etcd_wrapper_go_test)$",
        ],
    ]
    (HERE / "build-command.json").write_text(json.dumps(commands, indent=2) + "\n")
    for index, command in enumerate(commands):
        print("madsys-01 执行：", shlex.join(command), flush=True)
        script = f"""from pathlib import Path
import subprocess,os,json,time
cmd={command!r}; env=dict(os.environ,PATH={GO!r}+':'+os.environ['PATH'],GOMAXPROCS='1',GOPROXY='https://goproxy.cn,direct')
log=Path({REMOTE!r})/'build-{index}.log'
with log.open('w') as f:
 p=subprocess.Popen(cmd,env=env,stdout=f,stderr=subprocess.STDOUT)
 (Path({REMOTE!r})/'build-process.json').write_text(json.dumps({{'pid':p.pid,'command':cmd,'started':time.time()}}))
 rc=p.wait()
print(log.read_text()[-12000:])
raise SystemExit(rc)
"""
        print(remote_code("madsys-01", script, timeout=10800), flush=True)
    print(
        remote_code(
            "madsys-01",
            f"""from pathlib import Path
import shutil,json,subprocess,hashlib
b=Path({BUILD!r}); d=Path({REMOTE!r})
files={{'bin/mooncake_master':b/'mooncake-store/src/mooncake_master','bin/comparison-pressure':b/'mooncake-store/tools/mock_client/mooncake_durable_pressure','bin/mooncake_master_mock_client':b/'mooncake-store/tools/mock_client/mooncake_master_mock_client','lib/libetcd_wrapper.so':b/'mooncake-common/etcd/libetcd_wrapper.so','lib/libasio.so':b/'mooncake-common/libasio.so'}}
for dst,src in files.items(): shutil.copy2(src,d/dst)
manifest={{'source_commit':{commit!r},'source_archive_sha256':{source_sha!r},'build_host':'madsys-01','yalantinglibs_commit':{dependency_commit!r},'yalantinglibs_archive_sha256':{dependency_sha!r},'files':{{dst:hashlib.sha256((d/dst).read_bytes()).hexdigest() for dst in files}},'go_version':subprocess.check_output([{GO!r}+'/go','version'],text=True),'compiler':subprocess.check_output(['g++','--version'],text=True).splitlines()[0]}}
(d/'build-manifest.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest))
""",
        ),
        flush=True,
    )
    subprocess.run(
        [
            "scp",
            "-q",
            "madsys-01:" + REMOTE + "/build-manifest.json",
            str(HERE / "build-manifest.json"),
        ],
        check=True,
    )
    manifest = json.loads((HERE / "build-manifest.json").read_text())
    for rel, sha in manifest["files"].items():
        path = HERE / "deployment" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["scp", "-q", "madsys-01:" + REMOTE + "/" + rel, str(path)], check=True
        )
        assert hashlib.sha256(path.read_bytes()).hexdigest() == sha
        for host in HOSTS[1:]:
            subprocess.run(
                ["scp", "-q", str(path), host + ":" + REMOTE + "/" + rel], check=True
            )
            assert (
                ssh(host, "sha256sum " + shlex.quote(REMOTE + "/" + rel)).split()[0]
                == sha
            )
    print("三节点部署和 SHA256 校验完成", flush=True)


if __name__ == "__main__":
    main()
