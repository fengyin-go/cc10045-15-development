"""异常归因领域服务：对指标异常波动自动执行多维度归因和贡献度分析。

真实数据流：确定分析对象和异常时间点 → pipeline.detect_anomaly 执行 z-score 检测
→ 按归因维度解构贡献度 → 输出分析结论 → 写入 anomaly_analyses 表。

迟到数据恢复流程（本模块新增）：
告警再开启记录（alert_reopens）把迟到超限采样关联回已结案业务区间后，
异常归因通过 claim_reopen 领取当前待处理证据：原子地创建一条新调查，
新调查带齐新证据（迟到采样 id）和上一班的关闭依据（predecessor_closure）。
领取与重复检查可并发发生：再开启记录幂等去重，领取是单事务原子操作，
接班人不会丢失待办，也不会领取到两份调查。
旧的已关闭归因保持原样；新调查必须基于新证据真实跑检测才能结案，
不能借用旧结论直接关闭。
"""
from __future__ import annotations

import json
import threading
from typing import Any

from app.database import db
from app.pipeline import detect_anomaly
from app.services.alert_policy import REOPEN_CLAIMED, REOPEN_CLOSED, REOPEN_PENDING
from app.services.base import BaseModuleService

# 领取操作串行化：进程内锁 + IMMEDIATE 事务双保险，保证交班领取与
# 重复检查同时发生时，待办只被领取一次、调查只生成一份。
_claim_lock = threading.Lock()


class AnomalyAnalysisService(BaseModuleService):
    MODULE = "anomaly_analysis"
    ENTITY = "归因分析器"
    OPERATION_TERM = "分析"
    LIST_FIELDS = ["分析对象", "异常时间点", "归因维度", "贡献度排序", "分析结论"]
    REQUIRED_FIELDS = ["分析对象", "异常时间点", "归因维度"]
    STATUS_ORDER = ["待分析", "分析中", "已完成", "分析失败"]
    ACTION_RULES = {"启动分析": "已完成", "取消分析": "分析失败"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 交班领取：从告警再开启记录领取当前待处理证据
    # ------------------------------------------------------------------

    def claim_reopen(self, reopen_id: int, claimed_by: str) -> dict[str, Any]:
        """领取一条告警再开启记录，生成带齐证据的新调查。

        原子性：整个「读记录 → 建调查 → 标记已领取」在单个 IMMEDIATE
        事务内完成；重复领取（或领取与重复检查并发）不会产生第二份调查，
        再次领取返回同一条已关联的调查。
        """
        with _claim_lock, db.tx(immediate=True) as conn:
            reopen_row = conn.execute(
                "SELECT * FROM alert_reopens WHERE id=?", [reopen_id]
            ).fetchone()
            if reopen_row is None:
                conn.rollback()
                return {"ok": False, "message": f"告警再开启记录 {reopen_id} 不存在"}
            reopen = dict(reopen_row)

            if reopen["status"] != REOPEN_PENDING and reopen["analysis_id"]:
                # 已被领取：返回既有调查，保证不领取到两份
                existing = conn.execute(
                    "SELECT * FROM anomaly_analyses WHERE id=?",
                    [reopen["analysis_id"]],
                ).fetchone()
                return {
                    "ok": True,
                    "claimed": False,
                    "message": "该待办已被领取，返回同一份调查，不重复生成",
                    "reopen": self._reopen_view(reopen_id),
                    "investigation": dict(existing) if existing else None,
                }

            metric = conn.execute(
                "SELECT * FROM metrics WHERE id=?", [reopen["metric_id"]]
            ).fetchone()
            predecessor = None
            if reopen["previous_analysis_id"]:
                row = conn.execute(
                    "SELECT * FROM anomaly_analyses WHERE id=?",
                    [reopen["previous_analysis_id"]],
                ).fetchone()
                predecessor = dict(row) if row else None

            now = db.now()
            cursor = conn.execute(
                "INSERT INTO anomaly_analyses("
                "target_name, anomaly_time, attribution_dimensions, status,"
                " interval_key, evidence_sample_ids, reopen_id,"
                " predecessor_analysis_id, predecessor_closure, claimed_by, claimed_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                [
                    metric["metric_name"] if metric else "",
                    reopen["sample_time"],
                    (predecessor or {}).get("attribution_dimensions", "[]"),
                    "待分析",
                    reopen["interval_key"],
                    json.dumps([reopen["metric_value_id"]], ensure_ascii=False),
                    reopen_id,
                    reopen["previous_analysis_id"],
                    reopen["closure_evidence"],
                    claimed_by,
                    now,
                ],
            )
            analysis_id = cursor.lastrowid
            conn.execute(
                "UPDATE alert_reopens SET status=?, claimed_by=?, claimed_at=?,"
                " analysis_id=? WHERE id=?",
                [REOPEN_CLAIMED, claimed_by, now, analysis_id, reopen_id],
            )

        investigation = self.get_entry(analysis_id)
        return {
            "ok": True,
            "claimed": True,
            "message": f"已领取再开启待办，生成新调查 {analysis_id}（含新证据与上一班关闭依据）",
            "reopen": self._reopen_view(reopen_id),
            "investigation": investigation,
        }

    def _reopen_view(self, reopen_id: int) -> dict[str, Any] | None:
        row = db.fetchone("SELECT * FROM alert_reopens WHERE id=?", [reopen_id])
        if row is None:
            return None
        row["closure_evidence"] = _loads(row.get("closure_evidence"), {})
        return row

    # ------------------------------------------------------------------
    # 证据查看：本次归因与旧档分别查使用的指标采样
    # ------------------------------------------------------------------

    def investigation_samples(self, entry_id: int) -> dict[str, Any] | None:
        """返回一条调查使用的指标采样；若存在上一班旧档，分开列出旧档采样。

        主管打开本次归因看到的是新证据采样，打开旧档看到的仍是旧采样，
        两者互不污染。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        result: dict[str, Any] = {
            "entry_id": entry_id,
            "status": entry.get("status"),
            "interval_key": entry.get("interval_key"),
            "reopen_id": entry.get("reopen_id"),
            "claimed_by": entry.get("claimed_by"),
            "evidence_sample_ids": _loads(entry.get("evidence_sample_ids"), []),
            "samples": self._samples_for(entry),
            "predecessor": None,
        }

        predecessor_id = entry.get("predecessor_analysis_id")
        if predecessor_id:
            predecessor = self.get_entry(int(predecessor_id))
            if predecessor is not None:
                result["predecessor"] = {
                    "entry_id": predecessor["id"],
                    "status": predecessor.get("status"),
                    "conclusion": predecessor.get("conclusion"),
                    "evidence_sample_ids": _loads(predecessor.get("evidence_sample_ids"), []),
                    "samples": self._samples_for(predecessor),
                    "closure": _loads(entry.get("predecessor_closure"), {}),
                }
        return result

    @staticmethod
    def _samples_for(entry: dict[str, Any]) -> list[dict[str, Any]]:
        ids = _loads(entry.get("evidence_sample_ids"), [])
        samples: list[dict[str, Any]] = []
        for sample_id in ids:
            row = db.fetchone("SELECT * FROM metric_values WHERE id=?", [sample_id])
            if row is not None:
                samples.append(row)
        return samples

    # ------------------------------------------------------------------
    # 分析执行
    # ------------------------------------------------------------------

    def run_analysis(self, entry_id: int) -> dict[str, Any]:
        """执行异常归因分析。先加载记录，再调用管线。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"分析记录 {entry_id} 不存在"}

        # 标记为分析中
        db.update("anomaly_analyses", {"status": "分析中"}, "id=?", [entry_id])

        analysis_data = {
            "分析对象": entry.get("target_name") or entry.get("分析对象"),
            "异常时间点": entry.get("anomaly_time") or entry.get("异常时间点"),
            "归因维度": entry.get("attribution_dimensions") or entry.get("归因维度"),
        }
        result = detect_anomaly(analysis_data)

        if result.get("status") == "分析失败":
            db.update("anomaly_analyses", {"status": "分析失败"}, "id=?", [entry_id])
            return {"ok": False, "message": result.get("reason", "分析失败")}

        sample_ids = result.get("sample_ids", [])

        # 再开启来源的新调查：必须真实覆盖迟到采样才能结案，
        # 不能借用旧结论直接关闭。
        reopen_id = entry.get("reopen_id")
        if reopen_id:
            reopen = db.fetchone("SELECT * FROM alert_reopens WHERE id=?", [reopen_id])
            if reopen and reopen["metric_value_id"] not in sample_ids:
                db.update("anomaly_analyses", {"status": "待分析"}, "id=?", [entry_id])
                return {
                    "ok": False,
                    "message": (
                        f"迟到采样 {reopen['metric_value_id']} 未进入本次检测窗口，"
                        "新调查不能借用旧结论结案，已退回待分析"
                    ),
                }

        # 回写结论（结论来自本次检测，绝不复制上一班旧结论）
        conclusion = result.get("conclusion", "分析完成")
        contributions = result.get("contributions", {})
        ranking_json = json.dumps(
            sorted(contributions.items(), key=lambda x: -x[1])[:5],
            ensure_ascii=False,
        )

        interval_key = entry.get("interval_key")
        if not interval_key:
            metric = db.fetchone(
                "SELECT id FROM metrics WHERE metric_name LIKE ?",
                [f"%{analysis_data['分析对象']}%"],
            )
            interval_key = (
                f"interval:{metric['id']}:{str(analysis_data['异常时间点'])[:10]}"
                if metric
                else None
            )

        db.update("anomaly_analyses", {
            "status": "已完成",
            "contribution_ranking": ranking_json,
            "conclusion": conclusion,
            "evidence_sample_ids": json.dumps(sample_ids, ensure_ascii=False),
            "interval_key": interval_key,
        }, "id=?", [entry_id])

        # 调查结案后，关联的再开启记录同步结案
        if reopen_id:
            db.update("alert_reopens", {"status": REOPEN_CLOSED}, "id=?", [reopen_id])

        return {
            "ok": True,
            "target": analysis_data["分析对象"],
            "is_anomaly": result.get("is_anomaly", False),
            "z_score": result.get("z_score", 0),
            "conclusion": conclusion,
            "contributions": contributions,
            "sample_ids": sample_ids,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """再开启来源的调查不允许直接翻状态结案，必须走真实检测。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "启动分析" and entry.get("reopen_id"):
            result = self.run_analysis(entry_id)
            if not result.get("ok"):
                return None, result.get("message", "分析失败")
            return self.get_entry(entry_id), f"{self.ENTITY}已{action}（基于新证据检测）"
        return super().run_action(entry_id, action)

    def recent_anomalies(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的分析结果。"""
        return db.fetchall(
            "SELECT * FROM anomaly_analyses WHERE status='已完成' ORDER BY id DESC LIMIT ?",
            [limit],
        )

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_analyses"] = db.count("anomaly_analyses")
        snapshot["completed_analyses"] = db.count("anomaly_analyses", "status='已完成'")
        return snapshot


def _loads(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw) if raw else default
    except (json.JSONDecodeError, TypeError):
        return default
