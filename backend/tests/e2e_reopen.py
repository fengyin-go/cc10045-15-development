#!/usr/bin/env python3 -I
"""迟到数据恢复已关闭调查：端到端场景验证。

场景复现任务描述：
  原调查用第 17 号指标采样，关闭后第 19 号迟到且超限。
  连查两次 19 后只出现一个待办；打开老归因仍只看到 17；
  再领取新区间处理时要能找到 19 和上一班处置来源。
"""
from __future__ import annotations

import json
import threading
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8019"
PASS, FAIL = 0, 0


def api(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + urllib.parse.quote(path, safe="/?=&:_-"), data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}  {detail}")


def main() -> None:
    # ---- 准备：选一个种子指标，补录 16 条正常采样（id 1..16） ----
    metrics = api("GET", "/api/metric_store?size=50")["items"]
    metric = next(m for m in metrics if m["metric_name"] == "异常指标波动率")
    mid = metric["id"]
    print(f"指标: {metric['metric_name']} (id={mid})")

    for i in range(16):
        api("POST", f"/api/pipeline/metrics/{mid}/values", {
            "snapshot_value": 50 + (i % 3),
            "snapshot_time": f"2026-11-19T{i:02d}:00:00",
        })

    # ---- 告警规则：> 100，启用 ----
    r = api("POST", "/api/alert_policy", {"values": {
        "规则名称": "波动率超限告警", "监控指标": "异常指标波动率",
        "告警条件": "> 100", "通知方式": "email", "连续触发次数": "1",
    }})
    alert_id = r["entry"]["id"]
    api("POST", f"/api/alert_policy/{alert_id}/actions", {"action": "启用告警"})

    # ---- 第 17 号采样：超限，触发原业务区间的调查 ----
    s17 = api("POST", f"/api/pipeline/metrics/{mid}/values", {
        "snapshot_value": 200, "snapshot_time": "2026-11-20T10:00:00",
    })["sample"]
    assert s17["id"] == 17, f"期望第 17 号采样，实际 {s17['id']}"

    inv1 = api("POST", "/api/anomaly_analysis", {"values": {
        "分析对象": "异常指标波动率", "异常时间点": "2026-11-20T10:00:00",
        "归因维度": '["region"]', "贡献度排序": "", "分析结论": "",
    }})["entry"]
    run1 = api("POST", f"/api/pipeline/anomaly/{inv1['id']}/run")
    check("原调查结案", run1["ok"] and run1["is_anomaly"], json.dumps(run1, ensure_ascii=False))
    inv1_after = api("GET", f"/api/anomaly_analysis/{inv1['id']}")
    check("原调查证据含 17", "17" in (inv1_after.get("evidence_sample_ids") or ""))

    # ---- 值班长关闭这一区间的告警；另一人仍开着旧归因 ----
    api("POST", f"/api/alert_policy/{alert_id}/actions", {"action": "关闭告警"})

    # ---- 第 18 号：正常采样（不属于迟到超限）----
    # )
    s18 = api("POST", f"/api/pipeline/metrics/{mid}/values", {
        "snapshot_value": 52, "snapshot_time": "2026-11-21T09:00:00",
    })["sample"]
    assert s18["id"] == 18

    # ---- 第 19 号：迟到且超限，落在原业务区间 ----
    s19 = api("POST", f"/api/pipeline/metrics/{mid}/values", {
        "snapshot_value": 230, "snapshot_time": "2026-11-20T11:00:00",
    })["sample"]
    assert s19["id"] == 19

    print("== 迟到采样两次检查 ==")
    r1 = api("POST", f"/api/pipeline/alerts/{alert_id}/late-sample", {"metric_value_id": 19})
    check("首次检查生成再开启记录", r1["ok"] and r1["created"], json.dumps(r1, ensure_ascii=False))
    reopen_id = r1["reopen"]["id"]
    check("再开启记录关联原业务区间", r1["reopen"]["interval_key"] == inv1_after["interval_key"],
          f"{r1['reopen']['interval_key']} != {inv1_after['interval_key']}")
    check("再开启记录保留原关闭证据",
          r1["reopen"]["closure_evidence"].get("analysis_id") == inv1["id"]
          and "17" in json.dumps(r1["reopen"]["closure_evidence"].get("evidence_sample_ids", [])))

    r2 = api("POST", f"/api/pipeline/alerts/{alert_id}/late-sample", {"metric_value_id": 19})
    check("重复检查不产生新介入", r2["ok"] and not r2["created"])
    todos = api("GET", "/api/pipeline/alerts/reopens?status=待领取")
    check("连查两次 19 后只出现一个待办", todos["total"] == 1, f"total={todos['total']}")

    print("== 未确认采样不得触碰旧调查 ==")
    r3 = api("POST", f"/api/pipeline/alerts/{alert_id}/late-sample", {"metric_value_id": 18})
    check("未超限采样不产生介入", not r3["ok"] or not r3.get("created"))
    inv1_now = api("GET", f"/api/anomaly_analysis/{inv1['id']}")
    check("旧调查保持已关闭(已完成)", inv1_now["status"] == "已完成")
    check("旧调查结论未被改动", inv1_now["conclusion"] == inv1_after["conclusion"])

    print("== 旧档采样查看 ==")
    old_view = api("GET", f"/api/pipeline/anomaly/{inv1['id']}/samples")
    old_ids = [s["id"] for s in old_view["samples"]]
    check("打开老归因仍只看到 17（含 17 不含 19）", 17 in old_ids and 19 not in old_ids,
          f"old_ids={old_ids}")

    print("== 交班领取 ==")
    c1 = api("POST", f"/api/pipeline/anomaly/reopens/{reopen_id}/claim", {"claimed_by": "下一班-张三"})
    check("领取生成新调查", c1["ok"] and c1["claimed"], json.dumps(c1, ensure_ascii=False))
    inv2 = c1["investigation"]
    check("新调查带新证据 19", json.loads(inv2["evidence_sample_ids"]) == [19])
    check("新调查带上一班关闭依据",
          json.loads(inv2["predecessor_closure"]).get("analysis_id") == inv1["id"])
    check("新调查关联同一业务区间", inv2["interval_key"] == inv1_after["interval_key"])

    c2 = api("POST", f"/api/pipeline/anomaly/reopens/{reopen_id}/claim", {"claimed_by": "下一班-李四"})
    check("重复领取返回同一份调查", c2["ok"] and not c2["claimed"]
          and c2["investigation"]["id"] == inv2["id"])
    all_an = api("GET", "/api/anomaly_analysis?size=100")
    check("没有领取出两份调查",
          sum(1 for e in all_an["items"] if e.get("reopen_id") == reopen_id) == 1)

    print("== 并发：重复检查与交班领取同时发生 ==")
    s20 = api("POST", f"/api/pipeline/metrics/{mid}/values", {
        "snapshot_value": 240, "snapshot_time": "2026-11-20T12:00:00",
    })["sample"]
    results: list[dict] = []
    lock = threading.Lock()

    def worker(kind: str) -> None:
        if kind == "check":
            out = api("POST", f"/api/pipeline/alerts/{alert_id}/late-sample",
                      {"metric_value_id": s20["id"]})
        else:
            out = None
        with lock:
            results.append(out)

    threads = [threading.Thread(target=worker, args=("check",)) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    created_n = sum(1 for r in results if r and r.get("created"))
    check("并发重复检查只产生一次有效介入", created_n == 1, f"created={created_n}")
    reopen2_id = results[0]["reopen"]["id"]

    claim_outs: list[dict] = []

    def claimer(name: str) -> None:
        out = api("POST", f"/api/pipeline/anomaly/reopens/{reopen2_id}/claim",
                  {"claimed_by": name})
        with lock:
            claim_outs.append(out)

    threads = [threading.Thread(target=claimer, args=(f"班-{i}",)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    inv_ids = {c["investigation"]["id"] for c in claim_outs if c.get("investigation")}
    check("并发领取只得一份调查", len(inv_ids) == 1, f"inv_ids={inv_ids}")
    claimed_n = sum(1 for c in claim_outs if c.get("claimed"))
    check("并发领取只有一次成功领取", claimed_n == 1, f"claimed={claimed_n}")
    todos2 = api("GET", "/api/pipeline/alerts/reopens?status=待领取")
    check("交接后待办不丢失不重复", todos2["total"] == 0, f"total={todos2['total']}")

    print("== 并发混合：重复检查与交班领取同时发生 ==")
    s21 = api("POST", f"/api/pipeline/metrics/{mid}/values", {
        "snapshot_value": 250, "snapshot_time": "2026-11-20T13:00:00",
    })["sample"]
    first = api("POST", f"/api/pipeline/alerts/{alert_id}/late-sample",
                {"metric_value_id": s21["id"]})
    reopen3_id = first["reopen"]["id"]

    mixed: list[tuple[str, dict]] = []

    def mixed_check() -> None:
        out = api("POST", f"/api/pipeline/alerts/{alert_id}/late-sample",
                  {"metric_value_id": s21["id"]})
        with lock:
            mixed.append(("check", out))

    def mixed_claim(i: int) -> None:
        out = api("POST", f"/api/pipeline/anomaly/reopens/{reopen3_id}/claim",
                  {"claimed_by": f"混合班-{i}"})
        with lock:
            mixed.append(("claim", out))

    threads = []
    for i in range(3):
        threads.append(threading.Thread(target=mixed_check))
        threads.append(threading.Thread(target=mixed_claim, args=(i,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("混合并发检查均不产生新介入",
          all(not out.get("created") for kind, out in mixed if kind == "check"))
    mixed_inv = {out["investigation"]["id"] for kind, out in mixed
                 if kind == "claim" and out.get("investigation")}
    check("混合并发领取只得一份调查", len(mixed_inv) == 1, f"{mixed_inv}")
    all_an2 = api("GET", "/api/anomaly_analysis?size=200")
    check("混合并发后每个再开启记录仍只对应一份调查",
          sum(1 for e in all_an2["items"] if e.get("reopen_id") == reopen3_id) == 1)
    mixed_view = api("GET", f"/api/pipeline/anomaly/{list(mixed_inv)[0]}/samples")
    check("混合并发领到的调查带齐新证据",
          s21["id"] in [s["id"] for s in mixed_view["samples"]])
    check("混合并发领到的调查带上一班关闭依据",
          mixed_view["predecessor"] is not None
          and mixed_view["predecessor"]["entry_id"] == inv1["id"])

    print("== 新调查必须基于新证据结案 ==")
    act = api("POST", f"/api/anomaly_analysis/{inv2['id']}/actions", {"action": "启动分析"})
    check("再开启调查启动分析走真实检测", act["ok"], act.get("message", ""))
    inv2_after = api("GET", f"/api/anomaly_analysis/{inv2['id']}")
    check("新调查已结案", inv2_after["status"] == "已完成")
    check("新调查证据含 19", "19" in (inv2_after.get("evidence_sample_ids") or ""))
    check("新结论未借用旧结论", inv2_after["conclusion"] != inv1_after["conclusion"],
          inv2_after.get("conclusion") or "")

    new_view = api("GET", f"/api/pipeline/anomaly/{inv2['id']}/samples")
    new_ids = [s["id"] for s in new_view["samples"]]
    old_ids2 = [s["id"] for s in new_view["predecessor"]["samples"]]
    check("本次归因采样含 19", 19 in new_ids, f"new={new_ids}")
    check("旧档采样含 17 不含 19", 17 in old_ids2 and 19 not in old_ids2, f"old={old_ids2}")

    inv1_final = api("GET", f"/api/anomaly_analysis/{inv1['id']}")
    check("旧归因全程保持原样", inv1_final["status"] == "已完成"
          and inv1_final["conclusion"] == inv1_after["conclusion"]
          and inv1_final["evidence_sample_ids"] == inv1_after["evidence_sample_ids"])

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    raise SystemExit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
