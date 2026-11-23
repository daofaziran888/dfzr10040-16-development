"""窗口截止点事件时间校验测试。

场景：冲突规则以时间基准作为窗口截止点（12:00），同一键 K 的候选——
A 事件时间 11:55（窗内）、B 更晚到达但事件时间待核、C 事件时间 12:05（超窗）。
首次提交时事件时间证据冲突，质量门禁请求确认当前窗口，批次不放行；
补上 B 的事件时间 11:58 后重新提交，K 只保留一份经核的胜出明细，
C 仍不属于当前窗口。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine
from app.models import event_time_value


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def _make_job(engine: FusionEngine, cutoff: str = "12:00"):
    """建融合作业 + 窗口化冲突规则（生效中）+ 窗口确认门禁。"""
    job = engine.create_fusion_job(
        target_wide_table="WT_ORDER",
        participant_sources=["SRC-A", "SRC-B", "SRC-C"],
        join_key="order_id",
    )
    rule = engine.create_conflict_rule(
        target_wide_table="WT_ORDER",
        conflict_fields=["amount"],
        arbitration_strategy="windowed_event_time",
        time_baseline=cutoff,
    )
    rule.rule_status = "生效中"
    gate = engine.create_quality_gate(
        target_wide_table="WT_ORDER",
        check_type="window_confirmation",
        threshold_condition="",
    )
    return job, rule, gate


def _candidates() -> list[dict]:
    """K 的三个候选：A 窗内、B 待核、C 超窗。"""
    return [
        {"_fusion_key": "K", "_source": "SRC-A", "order_id": "K",
         "_event_time": "11:55", "_arrival_time": "11:56", "amount": 100},
        {"_fusion_key": "K", "_source": "SRC-B", "order_id": "K",
         "_arrival_time": "11:59", "amount": 200},  # 更晚到达，事件时间待核
        {"_fusion_key": "K", "_source": "SRC-C", "order_id": "K",
         "_event_time": "12:05", "_arrival_time": "12:01", "amount": 300},
    ]


class TestEventTimeParsing:
    """事件时间解析：缺失/不可解析一律视为待核。"""

    def test_hhmm_and_iso_parse(self) -> None:
        assert event_time_value("11:55") == 11 * 3600 + 55 * 60
        assert event_time_value("12:00:00") == 12 * 3600
        iso = event_time_value("2026-11-23T11:55:00")
        assert iso is not None and iso > 0

    def test_missing_or_invalid_is_unverifiable(self) -> None:
        assert event_time_value(None) is None
        assert event_time_value("") is None
        assert event_time_value("not-a-time") is None


class TestWindowedArbitration:
    """窗口化仲裁规则：窗口判定与证据保留。"""

    def test_window_verdicts_and_evidence_preserved(self, engine: FusionEngine) -> None:
        """每个候选保留原时间、窗口判定；胜出者保留实际胜出字段。"""
        _, rule, _ = _make_job(engine)
        result = rule.resolve_windowed(_candidates())

        evidence = {e["source"]: e for e in result["evidence"]}
        assert evidence["SRC-A"]["window_status"] == "窗内"
        assert evidence["SRC-A"]["event_time"] == "11:55"
        assert evidence["SRC-A"]["arrival_time"] == "11:56"
        assert evidence["SRC-B"]["window_status"] == "待核"
        assert evidence["SRC-B"]["event_time"] is None
        assert evidence["SRC-C"]["window_status"] == "超窗"
        assert evidence["SRC-C"]["event_time"] == "12:05"
        assert all(e["window_cutoff"] == "12:00" for e in evidence.values())

        # 可确认的胜出行只有 A；胜出字段为冲突字段 amount
        assert len(result["resolved"]) == 1
        winner = result["resolved"][0]
        assert winner["_source"] == "SRC-A"
        assert winner["_won_fields"] == ["amount"]
        assert evidence["SRC-A"]["is_winner"] is True
        assert evidence["SRC-A"]["won_fields"] == ["amount"]

        # 待核候选构成事件时间证据冲突，需要确认当前窗口
        assert result["window_confirmation_required"] is True
        assert [c["source"] for c in result["pending_candidates"]] == ["SRC-B"]
        assert [c["source"] for c in result["excluded_candidates"]] == ["SRC-C"]
        assert len(result["conflicts"]) == 1
        assert "到达时间不能替代源事件时间" in result["conflicts"][0]["reason"]

    def test_arrival_time_cannot_replace_event_time(self, engine: FusionEngine) -> None:
        """B 的到达时间在窗口内，但没有源事件时间仍为待核。"""
        _, rule, _ = _make_job(engine)
        records = [
            {"_fusion_key": "K", "_source": "SRC-B",
             "_arrival_time": "11:58", "amount": 200},
        ]
        result = rule.resolve_windowed(records)
        assert result["resolved"] == []
        assert result["window_confirmation_required"] is True
        assert result["evidence"][0]["window_status"] == "待核"
        assert result["evidence"][0]["arrival_time"] == "11:58"

    def test_out_of_window_candidate_excluded_from_confirmation(
        self, engine: FusionEngine,
    ) -> None:
        """超窗候选不进入本次确认，也不阻碍窗内候选胜出。"""
        _, rule, _ = _make_job(engine)
        records = [
            {"_fusion_key": "K", "_source": "SRC-A",
             "_event_time": "11:55", "amount": 100},
            {"_fusion_key": "K", "_source": "SRC-C",
             "_event_time": "12:05", "amount": 300},
        ]
        result = rule.resolve_windowed(records)
        assert len(result["resolved"]) == 1
        assert result["resolved"][0]["_source"] == "SRC-A"
        assert result["window_confirmation_required"] is False
        assert [e["source"] for e in result["excluded_candidates"]] == ["SRC-C"]

    def test_winner_picks_latest_event_time_not_arrival(
        self, engine: FusionEngine,
    ) -> None:
        """同键窗内候选按源事件时间取最新，与到达先后无关。"""
        _, rule, _ = _make_job(engine)
        records = [
            {"_fusion_key": "K", "_source": "SRC-A",
             "_event_time": "11:55", "_arrival_time": "11:59", "amount": 100},
            {"_fusion_key": "K", "_source": "SRC-B",
             "_event_time": "11:58", "_arrival_time": "11:57", "amount": 200},
        ]
        result = rule.resolve_windowed(records)
        assert len(result["resolved"]) == 1
        assert result["resolved"][0]["_source"] == "SRC-B"
        assert result["resolved"][0]["amount"] == 200


class TestWindowedSubmissionFlow:
    """提交 -> 门禁请求确认窗口 -> 检查可读快照 -> 补录后重新提交。"""

    def test_first_submission_requests_window_confirmation(
        self, engine: FusionEngine,
    ) -> None:
        """事件时间证据冲突时门禁请求确认当前窗口，批次不放行。"""
        job, _, gate = _make_job(engine)
        result = engine.submit_windowed_batch(job.job_id, _candidates())

        assert result["ok"] is True
        assert result["release_eligible"] is False
        assert result["window_confirmation_required"] is True
        assert result["gate_status"] == "请求确认当前窗口"
        assert "snapshot" not in result

        # 提交给门禁检查的是确认为可用的那批行（仅 A 的胜出行）
        assert result["confirmed_rows"] == 1
        assert gate.checked_rows == 1

        # 门禁关联仲裁时间证据判定发布资格
        gate_result = result["quality_gate"]
        assert gate_result["passed"] is False
        violation = gate_result["violations"][0]
        assert violation["check"] == "window_confirmation"
        assert violation["window_cutoff"] == "12:00"
        assert [c["source"] for c in violation["pending_candidates"]] == ["SRC-B"]
        assert gate_result["release_evidence"]["pending_candidates"] == 1
        assert gate_result["release_evidence"]["excluded_candidates"] == 1

        # 未能放行的候选：B 待核、C 超窗
        held = {c["source"]: c for c in result["held_candidates"]}
        assert held["SRC-B"]["window_status"] == "待核"
        assert held["SRC-C"]["window_status"] == "超窗"

    def test_inspect_readable_snapshot_and_held_candidates(
        self, engine: FusionEngine,
    ) -> None:
        """批次被扣留时可读快照保持旧值，未放行候选可核对。"""
        job, _, _ = _make_job(engine)
        # 上一批已放行：K 金额 90（事件时间 11:40）
        prior = [{"_fusion_key": "K", "_source": "SRC-A", "order_id": "K",
                  "_event_time": "11:40", "amount": 90}]
        released = engine.submit_windowed_batch(job.job_id, prior)
        assert released["release_eligible"] is True

        # 新批次证据冲突被扣留
        engine.submit_windowed_batch(job.job_id, _candidates())

        status = engine.inspect_release_status(job.job_id)
        assert status["ok"] is True
        # 可读快照里 K 仍是旧金额 90，未被扣留批次污染
        k_rows = [r for r in status["readable_records"] if r["_fusion_key"] == "K"]
        assert len(k_rows) == 1
        assert k_rows[0]["amount"] == 90
        # 未能放行的候选：B 待核（无事件时间）、C 超窗（12:05 > 12:00）
        held = {c["source"]: c for c in status["held_candidates"]}
        assert held["SRC-B"]["window_status"] == "待核"
        assert held["SRC-B"]["event_time"] is None
        assert held["SRC-B"]["arrival_time"] == "11:59"
        assert held["SRC-C"]["window_status"] == "超窗"
        assert held["SRC-C"]["event_time"] == "12:05"
        assert status["release_eligible"] is False

    def test_resubmit_after_supplement_releases_single_winner(
        self, engine: FusionEngine,
    ) -> None:
        """补上 B 的事件时间 11:58 后重新提交：K 只有一份经核胜出明细，C 仍超窗。"""
        job, _, _ = _make_job(engine)
        engine.submit_windowed_batch(job.job_id, _candidates())

        result = engine.supplement_event_time(
            job.job_id, "K", "11:58", source="SRC-B",
        )
        assert result["ok"] is True
        assert result["supplemented"] == {"fusion_key": "K", "event_time": "11:58"}
        assert result["release_eligible"] is True
        assert result["window_confirmation_required"] is False
        assert result["gate_status"] == "已放行"
        assert result["quality_gate"]["passed"] is True
        assert result["snapshot"]["status"] == "可用"

        status = engine.inspect_release_status(job.job_id)
        # K 只应有一份经核的胜出明细：B（事件时间 11:58 > A 的 11:55）
        k_rows = [r for r in status["readable_records"] if r["_fusion_key"] == "K"]
        assert len(k_rows) == 1
        winner = k_rows[0]
        assert winner["_source"] == "SRC-B"
        assert winner["_event_time"] == "11:58"
        assert winner["amount"] == 200
        assert winner["_won_fields"] == ["amount"]
        assert winner["_window_status"] == "窗内"

        # C 仍不属于当前窗口
        held = status["held_candidates"]
        assert len(held) == 1
        assert held[0]["source"] == "SRC-C"
        assert held[0]["window_status"] == "超窗"
        assert held[0]["event_time"] == "12:05"
        assert status["release_eligible"] is True

    def test_supplement_without_pending_batch_fails(self, engine: FusionEngine) -> None:
        """没有待确认批次时补录报错。"""
        job, _, _ = _make_job(engine)
        result = engine.supplement_event_time(job.job_id, "K", "11:58")
        assert result["ok"] is False
        assert "待确认" in result["message"]

    def test_iso_datetime_window_cutoff(self, engine: FusionEngine) -> None:
        """窗口截止点同样支持 ISO 日期时间格式。"""
        job, _, _ = _make_job(engine, cutoff="2026-11-23T12:00:00")
        candidates = [
            {"_fusion_key": "K", "_source": "SRC-A",
             "_event_time": "2026-11-23T11:55:00", "amount": 100},
            {"_fusion_key": "K", "_source": "SRC-C",
             "_event_time": "2026-11-23T12:05:00", "amount": 300},
        ]
        result = engine.submit_windowed_batch(job.job_id, candidates)
        assert result["release_eligible"] is True
        assert result["confirmed_rows"] == 1
        assert result["arbitration"]["resolved"][0]["_source"] == "SRC-A"
        assert [c["source"] for c in result["arbitration"]["excluded_candidates"]] == ["SRC-C"]


class TestPipelineWindowIntegration:
    """融合流水线同样携带窗口证据并阻断放行。"""

    def test_pipeline_blocks_release_on_pending_evidence(
        self, engine: FusionEngine,
    ) -> None:
        """流水线中窗口证据待核时不产出快照。"""
        engine.register_source(
            source_code="SRC-W", source_name="w_source", source_type="MySQL",
            connection_string="conn://w",
        )
        job = engine.create_fusion_job(
            target_wide_table="WT_PIPE",
            participant_sources=["SRC-W"],
            join_key="id",
        )
        rule = engine.create_conflict_rule(
            target_wide_table="WT_PIPE",
            conflict_fields=["name"],
            arbitration_strategy="windowed_event_time",
            time_baseline="12:00",
        )
        rule.rule_status = "生效中"
        engine.create_quality_gate(
            target_wide_table="WT_PIPE",
            check_type="window_confirmation",
            threshold_condition="",
        )

        # 模拟源读数不带 _event_time，全部候选待核
        result = engine.execute_fusion_pipeline(job.job_id)
        assert result["ok"] is True
        assert result["window_confirmation_required"] is True
        assert result["quality_gate"]["passed"] is False
        assert "snapshot" not in result
        assert len(result["held_candidates"]) > 0

    def test_resolve_dispatch_keeps_tuple_contract(self, engine: FusionEngine) -> None:
        """resolve() 对窗口化策略保持 (resolved, conflicts) 二元返回。"""
        _, rule, _ = _make_job(engine)
        resolved, conflicts = rule.resolve(_candidates())
        assert len(resolved) == 1
        assert resolved[0]["_source"] == "SRC-A"
        assert len(conflicts) == 1
