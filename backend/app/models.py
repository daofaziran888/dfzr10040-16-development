"""熔炉工坊数据模型：业务的 dataclass 定义，承载跨模块数据流对象。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


def event_time_value(raw: Any) -> float | None:
    """把事件时间解析为可比较的数值，支持 ISO 8601 与 HH:MM(:SS)。

    缺失、空串或无法解析时返回 None，调用方应视为「待核」，
    不允许用到达时间等其他字段顶替源事件时间。
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
            return float(parsed.hour * 3600 + parsed.minute * 60 + parsed.second)
        except ValueError:
            continue
    return None


@dataclass
class SourceDef:
    """数据源定义：维护多源异构数据的连接信息与认证配置。"""
    source_id: int
    source_code: str
    source_name: str
    source_type: str
    connection_string: str
    auth_method: str
    charset: str
    timezone: str
    access_status: str

    def to_record(self) -> dict[str, Any]:
        return {
            "源编号": self.source_code,
            "源名称": self.source_name,
            "源类型": self.source_type,
            "连接串": self.connection_string,
            "认证方式": self.auth_method,
            "字符编码": self.charset,
            "时区设置": self.timezone,
            "接入状态": self.access_status,
        }


@dataclass
class SchemaMapping:
    """Schema 映射：源表到目标宽表的字段映射、类型转换与缺失值策略。"""
    mapping_id: int
    plan_code: str
    source_table: str
    target_wide_table: str
    field_mapping_json: str
    type_conversion_rules: str
    missing_value_policy: str
    mapping_order: str
    plan_status: str

    def to_record(self) -> dict[str, Any]:
        return {
            "方案编号": self.plan_code,
            "源表名称": self.source_table,
            "目标宽表": self.target_wide_table,
            "字段映射JSON": self.field_mapping_json,
            "类型转换": self.type_conversion_rules,
            "缺失策略": self.missing_value_policy,
            "映射顺序": self.mapping_order,
            "方案状态": self.plan_status,
        }


@dataclass
class WideTableDef:
    """宽表定义：宽表元数据结构、字段清单与生命周期。"""
    table_id: int
    table_code: str
    table_name: str
    field_list: list[str]
    primary_key: str
    partition_key: str
    ttl_days: int
    source_count: int
    last_updated: str

    def to_record(self) -> dict[str, Any]:
        return {
            "表编号": self.table_code,
            "宽表名称": self.table_name,
            "字段列表": ",".join(self.field_list),
            "主键定义": self.primary_key,
            "分区键": self.partition_key,
            "TTL天数": str(self.ttl_days),
            "数据来源数": str(self.source_count),
            "最近更新": self.last_updated,
        }


@dataclass
class FusionJob:
    """融合作业：多源数据融合的编排、关联键与水位线管理。"""
    job_id: int
    job_code: str
    target_wide_table: str
    participant_sources: list[str]
    join_key: str
    fusion_window: str
    watermark_field: str
    output_rows: int = 0
    skipped_rows: int = 0
    merged_records: list[dict[str, Any]] = field(default_factory=list)
    # 窗口化提交的运行状态：已放行行构成可读快照，未放行候选保留窗口判定证据。
    released_records: list[dict[str, Any]] = field(default_factory=list)
    held_candidates: list[dict[str, Any]] = field(default_factory=list)
    pending_batch: list[dict[str, Any]] = field(default_factory=list)
    last_window_cutoff: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "作业编号": self.job_code,
            "目标宽表": self.target_wide_table,
            "参与数据源": ",".join(self.participant_sources),
            "关联键": self.join_key,
            "融合窗口": self.fusion_window,
            "水位线字段": self.watermark_field,
            "输出行数": str(self.output_rows),
            "跳过行数": str(self.skipped_rows),
        }


@dataclass
class MergeConflictRule:
    """冲突仲裁规则：多源融合时的字段冲突仲裁与优先级排序。"""
    rule_id: int
    rule_code: str
    target_wide_table: str
    conflict_fields: list[str]
    arbitration_strategy: str
    priority_source: str
    time_baseline: str
    conflict_count: int = 0
    rule_status: str = "草稿"

    def resolve(self, records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """执行冲突仲裁：按优先级来源与时间基准合并记录。"""
        if self.arbitration_strategy == "windowed_event_time":
            windowed = self.resolve_windowed(records)
            return windowed["resolved"], windowed["conflicts"]

        merged: dict[str, dict[str, Any]] = {}
        conflicts: list[dict[str, Any]] = []

        for rec in records:
            key = str(rec.get("_fusion_key", ""))
            if not key:
                continue
            if key in merged:
                existing = merged[key]
                conflict_resolved = self._arbitrate(existing, rec)
                if conflict_resolved is None:
                    conflicts.append({"existing": dict(existing), "incoming": dict(rec), "reason": "仲裁策略无法裁决"})
                else:
                    merged[key] = conflict_resolved
            else:
                merged[key] = dict(rec)

        return list(merged.values()), conflicts

    def resolve_windowed(self, records: list[dict[str, Any]],
                         cutoff: str | None = None) -> dict[str, Any]:
        """按窗口截止点校验事件时间的仲裁。

        - 窗口截止点取 ``time_baseline``（时间基准），可由 ``cutoff`` 显式覆盖。
        - 候选的 ``_event_time`` 是源事件时间；缺失或无法解析记为「待核」。
        - ``_arrival_time``/``_timestamp`` 是到达时间，仅作证据保留，
          不能替代源事件时间参与窗口判定。
        - 事件时间晚于截止点的候选记为「超窗」，不进入本次确认。
        - 每个融合键在「窗内」候选中取源事件时间最新者为胜出行。

        返回可确认的胜出行、证据冲突（待核候选）以及每个候选的仲裁时间
        证据（原事件时间、到达时间、窗口判定、是否胜出、实际胜出字段）。
        """
        window_cutoff = cutoff or self.time_baseline
        cutoff_value = event_time_value(window_cutoff)

        groups: dict[str, list[dict[str, Any]]] = {}
        for rec in records:
            key = str(rec.get("_fusion_key", ""))
            if not key:
                continue
            groups.setdefault(key, []).append(rec)

        resolved: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []

        for key, candidates in groups.items():
            entries: list[dict[str, Any]] = []
            for rec in candidates:
                event_raw = rec.get("_event_time")
                event_text = str(event_raw).strip() if event_raw is not None else ""
                event_num = event_time_value(event_raw)
                if event_num is None:
                    window_status = "待核"
                elif cutoff_value is not None and event_num > cutoff_value:
                    window_status = "超窗"
                else:
                    window_status = "窗内"
                entries.append({
                    "fusion_key": key,
                    "source": rec.get("_source", ""),
                    "event_time": event_text or None,
                    "arrival_time": rec.get("_arrival_time") or rec.get("_timestamp") or None,
                    "window_status": window_status,
                    "window_cutoff": window_cutoff,
                    "is_winner": False,
                    "won_fields": [],
                    "_record": rec,
                    "_event_num": event_num,
                })

            confirmed = [e for e in entries if e["window_status"] == "窗内"]
            if confirmed:
                # 同键窗内候选按源事件时间取最新；并列时保持先到先赢，结果确定。
                winner = max(confirmed, key=lambda e: e["_event_num"])
                winner["is_winner"] = True
                winner["won_fields"] = [f for f in self.conflict_fields if f in winner["_record"]]
                row = dict(winner["_record"])
                row["_window_status"] = "窗内"
                row["_arbitration"] = "windowed_event_time"
                row["_won_fields"] = list(winner["won_fields"])
                resolved.append(row)

            for entry in entries:
                if entry["window_status"] == "待核":
                    conflicts.append({
                        "fusion_key": key,
                        "source": entry["source"],
                        "reason": "事件时间待核，到达时间不能替代源事件时间",
                        "arrival_time": entry["arrival_time"],
                        "window_cutoff": window_cutoff,
                    })
                entry.pop("_record", None)
                entry.pop("_event_num", None)
                evidence.append(entry)

        return {
            "applied": True,
            "strategy": "windowed_event_time",
            "window_cutoff": window_cutoff,
            "resolved": resolved,
            "conflicts": conflicts,
            "evidence": evidence,
            "pending_candidates": [e for e in evidence if e["window_status"] == "待核"],
            "excluded_candidates": [e for e in evidence if e["window_status"] == "超窗"],
            "window_confirmation_required": any(e["window_status"] == "待核" for e in evidence),
        }

    def _arbitrate(self, a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any] | None:
        strategy = self.arbitration_strategy
        if strategy == "source_priority":
            return a if a.get("_source") == self.priority_source else b
        elif strategy == "latest_timestamp":
            ta = a.get("_timestamp", "")
            tb = b.get("_timestamp", "")
            return a if ta >= tb else b
        elif strategy == "version_select":
            try:
                va = int(a.get("_version", 0))
            except (ValueError, TypeError):
                va = 0
            try:
                vb = int(b.get("_version", 0))
            except (ValueError, TypeError):
                vb = 0
            return a if va >= vb else b
        elif strategy == "merge_complement":
            merged = dict(a)
            for field in self.conflict_fields:
                if not merged.get(field) and b.get(field):
                    merged[field] = b[field]
            return merged
        return None


@dataclass
class QualityGateRule:
    """质量门禁规则：宽表数据的质量检查与阻断策略。"""
    rule_id: int
    rule_code: str
    target_wide_table: str
    check_type: str
    threshold_condition: str
    trigger_action: str
    block_level: str
    checked_rows: int = 0
    last_triggered: str = ""


@dataclass
class SnapshotPartition:
    """分区快照：按日分区快照管理与版本标记。"""
    partition_id: int
    partition_code: str
    target_wide_table: str
    partition_key_value: str
    record_count: int
    schema_digest: str
    status_mark: str
    dependency_partitions: list[str]
    created_time: str

    def to_record(self) -> dict[str, Any]:
        return {
            "分区编号": self.partition_code,
            "目标宽表": self.target_wide_table,
            "分区键值": self.partition_key_value,
            "记录行数": str(self.record_count),
            "Schema摘要": self.schema_digest,
            "状态标记": self.status_mark,
            "依赖分区": ",".join(self.dependency_partitions),
            "创建时间": self.created_time,
        }


@dataclass
class BackfillTask:
    """数据回填任务：历史数据的批量回填与断点续传。"""
    task_id: int
    task_code: str
    target_wide_table: str
    backfill_range: str
    parallel_partitions: int
    dependency_check: str
    checkpoint_position: str = ""
    processed_rows: int = 0
    remaining_rows: int = 0


@dataclass
class ColdStoreStrategy:
    """冷热分层策略：宽表数据的冷热分层与自动迁移。"""
    strategy_id: int
    strategy_code: str
    target_wide_table: str
    hot_data_days: int
    cold_storage_type: str
    migration_window: str
    compression_method: str
    migrated_rows: int = 0
    last_migration: str = ""


@dataclass
class LineageEdge:
    """数据血缘边：宽表字段与源表字段之间的血缘追踪。"""
    edge_id: int
    edge_code: str
    upstream_node: str
    downstream_node: str
    related_fields: list[str]
    transform_function: str
    edge_type: str
    dependency_level: int
    last_traced: str


@dataclass
class MetricAggrRule:
    """指标聚合规则：宽表上的指标预聚合与物化视图。"""
    rule_id: int
    rule_code: str
    target_wide_table: str
    agg_function: str
    group_dimensions: list[str]
    time_granularity: str
    precompute_window: str
    storage_table: str
    last_computed: str = ""


@dataclass
class PartitionRule:
    """分区策略：宽表的物理分区策略与保留周期。"""
    rule_id: int
    strategy_code: str
    target_wide_table: str
    partition_type: str
    partition_field: str
    retention_days: int
    archive_policy: str
    partition_count: int
    last_adjusted: str


@dataclass
class WatermarkRecord:
    """水位追踪：各数据源的水位线追踪与消费延迟监控。"""
    record_id: int
    record_code: str
    data_source: str
    current_watermark: str
    previous_watermark: str
    advance_interval: str
    delay_minutes: int
    data_origin: str
    last_advance: str


@dataclass
class ExportPlan:
    """快照导出：宽表快照的按需导出与多格式转换。"""
    plan_id: int
    plan_code: str
    target_wide_table: str
    export_format: str
    partition_filter: str
    field_filter: list[str]
    export_path: str
    row_limit: int = 0
    export_status: str = "待导出"


@dataclass
class CrossValidatePlan:
    """交叉校验方案：多宽表间的交叉一致性校验。"""
    plan_id: int
    plan_code: str
    primary_wide_table: str
    reference_wide_table: str
    join_key: str
    validate_fields: list[str]
    deviation_threshold: str
    last_result: str = ""
    validate_status: str = "待校验"


@dataclass
class DataDriftRecord:
    """数据漂移记录：宽表数据的分布漂移检测。"""
    record_id: int
    record_code: str
    wide_table_name: str
    drift_type: str
    historical_baseline: str
    current_distribution: str
    kl_divergence: str
    drift_direction: str
    detected_at: str


@dataclass
class FusionMonitorMetric:
    """融合监控指标：数据融合管道的运行监控。"""
    metric_id: int
    metric_code: str
    fusion_job: str
    monitor_dimension: str
    collection_interval: str
    alert_threshold: str
    alert_channel: str
    silence_window: str
    last_sample: str = ""