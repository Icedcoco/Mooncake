"""持续更新报告入口，控制器结束后汇总两轮结果；不改变远端测试状态。"""

import datetime
import json
from pathlib import Path
import sys
import time


HERE = Path(__file__).resolve().parent


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def render(label):
    suite = read(HERE / (label + "-suite.json"))
    now = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
    text = [
        "# 两轮各两小时 durable 性能测试",
        "",
        f"更新：{now}；当前编排状态：**{suite.get('phase', '启动中')}**。",
        "",
        "先执行 put+evict，再重新建立专用集群执行 put+remove。每轮目标供压7200秒，逐分钟CSV各120行。",
        "正在运行时此文件为进度入口，不代表已完成测试；未完成的轮次没有最终吞吐结论。",
        "",
        "配置：2 workers、BatchPut 128、OpLog batch 8192、1KiB对象、256MiB segment、82字节key前缀；HA+OpLog+Snapshot开启；standby单页最多16 batch；prune 10 batch/块、50 batch/s；etcd periodic 5m、compaction limit8/sleep100ms。",
        "",
        "| 场景 | 状态 | 实际供压秒数 | durable PUT_END/s | durable REMOVE/s | eviction对象数 | 审计对齐 |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    issues = [
        "# 两轮测试非预期问题汇总",
        "",
        "以下内容随每轮归档更新；正在运行的轮次尚未形成最终问题结论。",
        "",
    ]
    links = []
    for workload in ("put-evict", "put-remove"):
        case = label + "-" + workload
        root = HERE / case
        run = read(root / "run.json")
        summary = read(root / "summary.json")
        status = read(root / "live-status.json")
        progress = status.get("madsys-03", {}).get("client", {}).get("elapsed", 0)
        seconds = summary.get("actual_seconds", progress)
        numbers = [
            f"{summary[k]:.3f}" if k in summary else "待结束"
            for k in ("put_per_s", "remove_per_s")
        ]
        text.append(
            f"| {workload} | {run.get('phase', '等待上一轮结束')} | {seconds:.1f} | "
            + " | ".join(numbers)
            + f" | {summary.get('evicted_keys', '待结束')} | {summary.get('audit_complete', '待核验')} |"
        )
        for filename, title in [
            ("REPORT.md", "测试报告"),
            ("per-minute.csv", "逐分钟CSV"),
            ("ISSUES.md", "问题报告"),
        ]:
            if (root / filename).exists():
                links.append(f"- {workload}：[ {title} ]({case}/{filename})")
        if (root / "ISSUES.md").exists():
            issues += [f"## {workload}", "", (root / "ISSUES.md").read_text(), ""]
        if run.get("error"):
            issues += [f"{workload} 控制器错误：`{run['error']}`。", ""]
        if run.get("report_error"):
            issues += [f"{workload} 报告生成失败：`{run['report_error']}`。", ""]
    text += [
        "",
        *links,
        "",
        "durable来自etcd中OpLog与durable_prefix同revision的审计确认，PUT_END类型为1、REMOVE为3；回调成功数量不作为durable吞吐。收尾entry单列，不混入供压时段。",
        "",
    ]
    if suite.get("error"):
        issues += ["编排错误：" + suite["error"], ""]
    for name, lines in [("REPORT.md", text), ("ISSUES.md", issues)]:
        temporary = HERE / (name + ".tmp")
        temporary.write_text("\n".join(lines))
        temporary.replace(HERE / name)
    return suite.get("phase") in ("complete", "incomplete", "failed")


if __name__ == "__main__":
    label = sys.argv[1]
    deadline = time.monotonic() + 6 * 3600
    while time.monotonic() < deadline:
        if render(label):
            break
        time.sleep(30)
    else:
        with (HERE / "ISSUES.md").open("a") as out:
            out.write("\n超过6小时仍未取得编排终态，需检查控制器与远端看门狗记录。\n")
