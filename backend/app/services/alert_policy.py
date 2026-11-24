"""分析告警策略领域服务：定义指标的异常波动和阈值告警策略。

真实数据流：定义告警规则（监控指标/告警条件/连续触发次数/通知方式）
→ pipeline.check_alert 从 metric_values 读取最新值 → 对比告警条件
→ 达到连续阈值时触发告警日志 → 记录到 alert_log → 下游推送执行。

迟到数据恢复流程（本模块新增）：
同一指标的迟到采样补录进 metric_values 后，若其采样时间落在已经结案的业务区间，
register_late_sample 会把这条新超限采样登记为一条「告警再开启记录」
（alert_reopens 表），关联回原业务区间与已结案调查，并把原关闭证据快照
一并保存。旧的已关闭归因保持原样，不会被改成未关闭。
同一新样本由 dedupe_key(alert_id:metric_value_id) 幂等去重：
重复检查只产生一次有效介入（待办只有一条）。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import check_alert, evaluate_condition
from app.services.base import BaseModuleService

# 再开启记录状态
REOPEN_PENDING = "待领取"
REOPEN_CLAIMED = "已领取"
REOPEN_CLOSED = "已结案"


class AlertPolicyService(BaseModuleService):
    MODULE = "alert_policy"
    ENTITY = "告警规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "监控指标", "告警条件", "通知方式", "连续触发次数"]
    REQUIRED_FIELDS = ["规则名称", "监控指标", "告警条件"]
    STATUS_ORDER = ["待生效", "已生效", "已关闭"]
    ACTION_RULES = {"启用告警": "已生效", "关闭告警": "已关闭"}
    NEGATIVE_ACTIONS = ["关闭告警"]

    def check_all(self) -> dict[str, Any]:
        """检查所有已生效告警规则。"""
        alerts = db.fetchall("SELECT * FROM alerts WHERE status='已生效'")
        results = []
        triggered_count = 0

        for alert in alerts:
            result = check_alert(dict(alert))
            if result:
                results.append({
                    "rule_name": alert["rule_name"],
                    "triggered": result.get("triggered", False),
                    "message": result.get("message", ""),
                })
                if result.get("triggered"):
                    triggered_count += 1

        return {
            "checked": len(alerts),
            "triggered": triggered_count,
            "details": results,
        }

    def check_single(self, alert_id: int) -> dict[str, Any]:
        """检查单个告警规则。"""
        alert = db.fetchone("SELECT * FROM alerts WHERE id=?", [alert_id])
        if alert is None:
            return {"ok": False, "message": f"告警规则 {alert_id} 不存在"}

        result = check_alert(dict(alert))
        return {
            "ok": True,
            "rule_name": alert["rule_name"],
            "check_result": result,
        }

    def alert_history(self, alert_id: int, limit: int = 50) -> list[dict[str, Any]]:
        """获取告警触发历史。"""
        return db.fetchall(
            "SELECT * FROM alert_log WHERE alert_id=? ORDER BY triggered_at DESC LIMIT ?",
            [alert_id, limit],
        )

    def resolve_alert(self, log_id: int) -> dict[str, Any]:
        """标记告警为已解决。"""
        db.update("alert_log", {"resolved": 1}, "id=?", [log_id])
        return {"ok": True, "message": "告警已标记为已解决"}

    # ------------------------------------------------------------------
    # 迟到数据恢复：告警再开启记录
    # ------------------------------------------------------------------

    def register_late_sample(
        self,
        alert_id: int,
        metric_value_id: int,
        interval_key: str | None = None,
    ) -> dict[str, Any]:
        """登记迟到采样，必要时为已结案业务区间生成告警再开启记录。

        幂等：同一 (alert_id, metric_value_id) 重复检查只产生一次有效介入，
        后续检查返回 created=False 且待办数量不变。

        未确认的新采样（未超出告警条件）不会创建再开启记录，也不会触碰
        旧的已关闭调查——旧调查始终保持已结案状态。
        """
        alert = db.fetchone("SELECT * FROM alerts WHERE id=?", [alert_id])
        if alert is None:
            return {"ok": False, "message": f"告警规则 {alert_id} 不存在"}

        metric = db.fetchone(
            "SELECT * FROM metrics WHERE metric_name LIKE ?",
            [f"%{alert['monitor_metric']}%"],
        )
        if metric is None:
            return {"ok": False, "message": f"监控指标 {alert['monitor_metric']} 未登记"}

        sample = db.fetchone(
            "SELECT * FROM metric_values WHERE id=? AND metric_id=?",
            [metric_value_id, metric["id"]],
        )
        if sample is None:
            return {
                "ok": False,
                "message": f"指标采样 {metric_value_id} 不存在或不属于指标 {metric['metric_name']}",
            }

        # 未确认的新采样：不超限则不产生介入，旧调查维持已关闭
        if not evaluate_condition(sample["snapshot_value"], alert["alert_condition"]):
            return {
                "ok": False,
                "created": False,
                "message": (
                    f"采样 {metric_value_id} 未超出告警条件（{alert['alert_condition']}），"
                    "属于未确认采样，不产生再开启介入"
                ),
            }

        # 关联已结案的业务区间与原调查（只读旧档，绝不改写）
        previous = self._find_closed_investigation(metric["id"], interval_key)
        if interval_key is None:
            if previous and previous.get("interval_key"):
                interval_key = previous["interval_key"]
            else:
                interval_key = f"interval:{metric['id']}:{sample['snapshot_time'][:10]}"

        closure_evidence = self._closure_evidence(previous)

        dedupe_key = f"{alert_id}:{metric_value_id}"
        cursor = db.execute(
            "INSERT OR IGNORE INTO alert_reopens("
            "dedupe_key, alert_id, interval_key, metric_id, metric_value_id,"
            " metric_value, sample_time, previous_analysis_id, closure_evidence, status"
            ") VALUES(?,?,?,?,?,?,?,?,?,?)",
            [
                dedupe_key,
                alert_id,
                interval_key,
                metric["id"],
                metric_value_id,
                sample["snapshot_value"],
                sample["snapshot_time"],
                previous["id"] if previous else None,
                json.dumps(closure_evidence, ensure_ascii=False),
                REOPEN_PENDING,
            ],
        )
        created = (cursor.rowcount or 0) > 0

        reopen = db.fetchone("SELECT * FROM alert_reopens WHERE dedupe_key=?", [dedupe_key])
        if reopen is not None:
            reopen["closure_evidence"] = _loads(reopen.get("closure_evidence"), {})
        return {
            "ok": True,
            "created": created,
            "message": (
                "迟到采样超限，已生成告警再开启记录"
                if created
                else "该迟到采样已登记过再开启记录，本次检查不产生新的介入"
            ),
            "reopen": reopen,
        }

    def list_reopens(
        self,
        status: str | None = None,
        interval_key: str | None = None,
    ) -> list[dict[str, Any]]:
        """列出告警再开启记录（下一班待办），附带规则名与指标名。"""
        where = ["1=1"]
        params: list[Any] = []
        if status:
            where.append("r.status = ?")
            params.append(status)
        if interval_key:
            where.append("r.interval_key = ?")
            params.append(interval_key)
        rows = db.fetchall(
            "SELECT r.*, a.rule_name AS alert_rule_name, m.metric_name "
            "FROM alert_reopens r "
            "JOIN alerts a ON a.id = r.alert_id "
            "JOIN metrics m ON m.id = r.metric_id "
            f"WHERE {' AND '.join(where)} ORDER BY r.id DESC",
            params,
        )
        for row in rows:
            row["closure_evidence"] = _loads(row.get("closure_evidence"), {})
        return rows

    def get_reopen(self, reopen_id: int) -> dict[str, Any] | None:
        """再开启记录详情：新采样 + 原关闭证据 + 领取/调查关联。"""
        row = db.fetchone(
            "SELECT r.*, a.rule_name AS alert_rule_name, m.metric_name "
            "FROM alert_reopens r "
            "JOIN alerts a ON a.id = r.alert_id "
            "JOIN metrics m ON m.id = r.metric_id WHERE r.id=?",
            [reopen_id],
        )
        if row is None:
            return None
        row["closure_evidence"] = _loads(row.get("closure_evidence"), {})
        return row

    def _find_closed_investigation(
        self,
        metric_id: int,
        interval_key: str | None,
    ) -> dict[str, Any] | None:
        """查找该指标在业务区间内最近一条已结案（已完成）的归因调查。"""
        metric = db.fetchone("SELECT metric_name FROM metrics WHERE id=?", [metric_id])
        if metric is None:
            return None
        where = ["status = '已完成'", "(target_name = ? OR ? LIKE '%'||target_name||'%')"]
        params: list[Any] = [metric["metric_name"], metric["metric_name"]]
        if interval_key:
            where.append("interval_key = ?")
            params.append(interval_key)
        return db.fetchone(
            f"SELECT * FROM anomaly_analyses WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT 1",
            params,
        )

    @staticmethod
    def _closure_evidence(previous: dict[str, Any] | None) -> dict[str, Any]:
        """把原调查的关闭证据做成快照，随再开启记录一起保留。"""
        if not previous:
            return {}
        return {
            "analysis_id": previous["id"],
            "status": previous["status"],
            "conclusion": previous.get("conclusion"),
            "contribution_ranking": previous.get("contribution_ranking"),
            "evidence_sample_ids": _loads(previous.get("evidence_sample_ids"), []),
            "interval_key": previous.get("interval_key"),
            "anomaly_time": previous.get("anomaly_time"),
            "closed_at": previous.get("created_at"),
            "evidence_captured_at": db.now(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_rules"] = db.count("alerts")
        snapshot["active_rules"] = db.count("alerts", "status='已生效'")
        snapshot["today_triggers"] = db.count("alert_log")
        snapshot["pending_reopens"] = db.count("alert_reopens", "status=?", [REOPEN_PENDING])
        return snapshot


def _loads(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw) if raw else default
    except (json.JSONDecodeError, TypeError):
        return default
