"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


# ---------------------------------------------------------------------------
# 迟到数据恢复：告警再开启 → 归因领取 → 证据查看
# ---------------------------------------------------------------------------

@router.post("/metrics/{metric_id}/values")
def api_backfill_metric_value(metric_id: int, payload: dict[str, Any] = Body(...)) -> dict:
    """补录一条指标采样（迟到数据入口）。

    body: {"snapshot_value": 数值, "snapshot_time": "YYYY-MM-DDTHH:MM:SS"(可选, 默认当前)}
    迟到采样按业务时间写入，供告警再开启流程引用。
    """
    metric = db.fetchone("SELECT * FROM metrics WHERE id=?", [metric_id])
    if metric is None:
        return {"ok": False, "message": f"指标 {metric_id} 不存在"}
    value = payload.get("snapshot_value")
    if value is None:
        return {"ok": False, "message": "缺少 snapshot_value"}
    sample_time = str(payload.get("snapshot_time") or db.now())
    sample_id = db.insert("metric_values", {
        "metric_id": metric_id,
        "snapshot_value": float(value),
        "snapshot_time": sample_time,
        "calibration_version": metric.get("calibration_version", 1),
        "comparison_status": "已保存",
    })
    sample = db.fetchone("SELECT * FROM metric_values WHERE id=?", [sample_id])
    return {"ok": True, "sample": sample}


@router.post("/alerts/{alert_id}/late-sample")
def api_register_late_sample(alert_id: int, payload: dict[str, Any] = Body(...)) -> dict:
    """登记迟到采样检查：超限且落在已结案区间时生成告警再开启记录。

    body: {"metric_value_id": 采样id, "interval_key": 业务区间(可选)}
    幂等：同一采样重复检查只产生一次有效介入。
    """
    svc = service_for("alert_policy")
    metric_value_id = payload.get("metric_value_id")
    if metric_value_id is None:
        return {"ok": False, "message": "缺少 metric_value_id"}
    return svc.register_late_sample(
        alert_id,
        int(metric_value_id),
        interval_key=payload.get("interval_key"),
    )


@router.get("/alerts/reopens")
def api_list_reopens(
    status: str | None = Query(default=None),
    interval_key: str | None = Query(default=None),
) -> dict:
    """列出告警再开启记录（下一班待办），含前任关闭证据。"""
    svc = service_for("alert_policy")
    items = svc.list_reopens(status=status, interval_key=interval_key)
    return {"items": items, "total": len(items)}


@router.get("/alerts/reopens/{reopen_id}")
def api_get_reopen(reopen_id: int) -> dict:
    """再开启记录详情：新采样、原关闭证据、领取状态。"""
    svc = service_for("alert_policy")
    reopen = svc.get_reopen(reopen_id)
    if reopen is None:
        return {"ok": False, "message": f"告警再开启记录 {reopen_id} 不存在"}
    return {"ok": True, "reopen": reopen}


@router.post("/anomaly/reopens/{reopen_id}/claim")
def api_claim_reopen(reopen_id: int, payload: dict[str, Any] | None = Body(default=None)) -> dict:
    """交班领取：原子领取再开启待办，生成带齐新证据与上一班关闭依据的新调查。"""
    svc = service_for("anomaly_analysis")
    claimed_by = str((payload or {}).get("claimed_by") or "下一班")
    return svc.claim_reopen(reopen_id, claimed_by)


@router.get("/anomaly/{entry_id}/samples")
def api_investigation_samples(entry_id: int) -> dict:
    """查看调查使用的指标采样：本次归因与旧档分开返回。"""
    svc = service_for("anomaly_analysis")
    result = svc.investigation_samples(entry_id)
    if result is None:
        return {"ok": False, "message": f"归因分析器 {entry_id} 不存在"}
    return {"ok": True, **result}


@router.post("/anomaly/{entry_id}/run")
def api_run_analysis(entry_id: int) -> dict:
    """执行归因分析（再开启来源的调查必须基于新证据真实检测）。"""
    svc = service_for("anomaly_analysis")
    return svc.run_analysis(entry_id)


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}