"""校准档案更正预演（只读）测试。

覆盖：
- 预演：原档案/拟定档案双份重算摘要与异常数、digest_changed / anomalies_changed
  标记（异常列表不变但档案内容变 → digest 仍变化）；
- 影响面：仅对摘要变化批次列出绑定旧摘要的 case_id 与关联 shipment_id，
  各自排序去重；绑定其他/过期摘要的案件与未涉及批次的联单不列入；
- 只读：预演不修改档案、批次异常行、案件与联单；同请求重复预演结果稳定，
  现有分析/复核/联单接口与幂等行为保持可用；
- 拒绝：档案不存在 409、批次不存在 404（指出对象）、批次属于其他档案/其他
  设备 409、batch_ids 重复/越界与区间违规 422、拟定区间漏掉任一采样 422
  （指出批次与采样时刻）且整次拒绝。
"""

import json
import os
import sqlite3
import tempfile
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-preview-"), "preview.db")
os.environ["COLDCHAIN_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.main import app  # noqa: E402


def iso(epoch_seconds) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def iso_out(epoch_seconds) -> str:
    return iso(epoch_seconds).replace("Z", ".000Z")


DEVICE = "reefer-preview-01"
DEVICE_OTHER = "reefer-preview-other"
PROFILE = "PREV-P1"        # [0,1800) scale=1.0 offset=0.5
PROFILE_OTHER = "PREV-P2"  # 另一设备档案

ANALYZE_HEADERS = {"Content-Type": "application/json"}


def profile_body(profile_id=PROFILE, device_id=DEVICE, intervals=None):
    if intervals is None:
        intervals = [
            {"start": iso(0), "end": iso(1800), "scale": 1.0, "offset": 0.5},
        ]
    return {"profile_id": profile_id, "device_id": device_id,
            "intervals": intervals}


def preview_body(profile_id=PROFILE, intervals=None, batch_ids=None):
    if intervals is None:
        # 拟定：offset 0.5 → 0.6（仅覆盖 [0,1200)）
        intervals = [
            {"start": iso(0), "end": iso(1200), "scale": 1.0, "offset": 0.6},
        ]
    return {"profile_id": profile_id, "intervals": intervals,
            "batch_ids": list(batch_ids or [])}


def analysis_body(batch_id, profile_id=PROFILE, device_id=DEVICE,
                  samples=None, segments=None, gap_seconds=600,
                  dip_tolerance_seconds=120):
    if samples is None:
        # 8.0 校准 8.5 越界、5.0 短暂回落、9.0 校准后 9.5 为峰值：1 条异常
        samples = [
            {"timestamp": iso(0), "temperature": 8.0},
            {"timestamp": iso(60), "temperature": 5.0},
            {"timestamp": iso(120), "temperature": 9.0},
        ]
    if segments is None:
        segments = [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(1800),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ]
    return {
        "batch_id": batch_id,
        "device_id": device_id,
        "profile_id": profile_id,
        "gap_seconds": gap_seconds,
        "dip_tolerance_seconds": dip_tolerance_seconds,
        "samples": samples,
        "segments": segments,
    }


class CorrectionPreviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)
        # 原档案：[0,1800) +0.5
        r = cls.client.post(
            "/api/v1/calibration-profiles", json=profile_body()
        )
        assert r.status_code == 200, r.text
        # 另一设备的档案
        r = cls.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(profile_id=PROFILE_OTHER, device_id=DEVICE_OTHER),
        )
        assert r.status_code == 200, r.text

        # B1：1 条异常（offset 变化只改峰值温度，异常结构不变）
        r = cls.client.post(
            "/api/v1/batches/analyze", json=analysis_body("PREV-B1")
        )
        assert r.status_code == 200, r.text
        cls.b1 = r.json()
        cls.d1 = cls.b1["analysis_digest"]

        # B2：7.45 +0.5=7.95 在限内（原档案 0 条异常），+0.6=8.05 越界
        # （拟定档案新增 1 条）；5.0 校准后仍在限内。
        b2_samples = [
            {"timestamp": iso(0), "temperature": 7.45},
            {"timestamp": iso(60), "temperature": 5.0},
        ]
        r = cls.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("PREV-B2", samples=b2_samples),
        )
        assert r.status_code == 200, r.text
        cls.b2 = r.json()
        cls.d2 = cls.b2["analysis_digest"]
        assert cls.b2["anomaly_count"] == 0, cls.b2

        # B3：无异常批次（原始值全在限内且 +0.5/+0.6 均不越界）
        b3_samples = [
            {"timestamp": iso(0), "temperature": 5.0},
            {"timestamp": iso(60), "temperature": 6.0},
        ]
        r = cls.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("PREV-B3", samples=b3_samples),
        )
        assert r.status_code == 200, r.text
        cls.b3 = r.json()
        cls.d3 = cls.b3["analysis_digest"]
        assert cls.b3["anomaly_count"] == 0

    # ---------- 基本双份重算与变化标记 ----------

    def test_01_preview_returns_two_snapshots_and_flags(self):
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B1", "PREV-B2", "PREV-B3"]),
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["profile_id"], PROFILE)
        self.assertEqual(body["device_id"], DEVICE)
        # 拟定区间回显（排序后）
        self.assertEqual(
            body["intervals"],
            [{"start": iso_out(0), "end": iso_out(1200),
              "scale": 1.0, "offset": 0.6}],
        )
        by_id = {b["batch_id"]: b for b in body["batches"]}
        self.assertEqual(
            [b["batch_id"] for b in body["batches"]],
            ["PREV-B1", "PREV-B2", "PREV-B3"],
        )

        b1 = by_id["PREV-B1"]
        # 原档案快照与既有分析响应一致
        self.assertEqual(b1["current"]["analysis_digest"], self.d1)
        self.assertEqual(b1["current"]["anomaly_count"], 1)
        # offset 改变 → 摘要变化；校准后峰值 9.5→9.6，异常内容也变，
        # 但异常结构不变（起止、数量均一致，仍 1 条）
        self.assertNotEqual(
            b1["proposed"]["analysis_digest"], b1["current"]["analysis_digest"]
        )
        self.assertEqual(b1["proposed"]["anomaly_count"], 1)
        self.assertTrue(b1["digest_changed"])
        self.assertTrue(b1["anomalies_changed"])

        b2 = by_id["PREV-B2"]
        self.assertEqual(b2["current"]["analysis_digest"], self.d2)
        self.assertEqual(b2["current"]["anomaly_count"], 0)
        # 7.45+0.6=8.05 越界 → 拟定下新增 1 条异常
        self.assertEqual(b2["proposed"]["anomaly_count"], 1)
        self.assertTrue(b2["digest_changed"])
        self.assertTrue(b2["anomalies_changed"])

        # B3 异常列表两边都为空，但档案内容变了 → 摘要仍变化、异常列表不变
        b3 = by_id["PREV-B3"]
        self.assertEqual(b3["current"]["anomaly_count"], 0)
        self.assertEqual(b3["proposed"]["anomaly_count"], 0)
        self.assertTrue(b3["digest_changed"])
        self.assertFalse(b3["anomalies_changed"])

    def test_01b_nonempty_identical_anomalies_still_flags_digest_change(self):
        """关键语义：异常（含温度值）逐条一致，仅档案描述变化 → 摘要仍变化。

        拟定区间在无采样时段 [1200,1800) 另立一段（系数不同），批内采样
        全部落在 [0,1200) 且系数不变，因此校准结果与异常列表逐字节一致，
        但 profile 描述参与摘要，digest 必须变化并列入影响面。
        """
        split_intervals = [
            {"start": iso(0), "end": iso(1200), "scale": 1.0, "offset": 0.5},
            {"start": iso(1200), "end": iso(1800), "scale": 1.2, "offset": 0.9},
        ]
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(intervals=split_intervals, batch_ids=["PREV-B1"]),
        )
        self.assertEqual(r.status_code, 200, r.text)
        b1 = r.json()["batches"][0]
        self.assertFalse(b1["anomalies_changed"])
        self.assertTrue(b1["digest_changed"])
        self.assertNotEqual(
            b1["current"]["analysis_digest"], b1["proposed"]["analysis_digest"]
        )

    def test_02_identical_intervals_mean_no_change(self):
        same = [
            {"start": iso(0), "end": iso(1800), "scale": 1.0, "offset": 0.5},
        ]
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(intervals=same, batch_ids=["PREV-B1"]),
        )
        self.assertEqual(r.status_code, 200, r.text)
        b1 = r.json()["batches"][0]
        self.assertFalse(b1["digest_changed"])
        self.assertFalse(b1["anomalies_changed"])
        self.assertEqual(
            b1["current"]["analysis_digest"],
            b1["proposed"]["analysis_digest"],
        )
        self.assertEqual(b1["affected_case_ids"], [])
        self.assertEqual(b1["affected_shipment_ids"], [])
        self.assertEqual(r.json()["affected_case_ids"], [])
        self.assertEqual(r.json()["affected_shipment_ids"], [])

    # ---------- 影响面：案件与联单 ----------

    def test_03_affected_cases_and_shipments_sorted_dedup(self):
        # 对 B1 用旧摘要建两个案件（案件号只要求唯一，均可绑定当前快照）
        for cid in ["CASE-Z", "CASE-A"]:
            r = self.client.post(
                "/api/v1/cases/review",
                json={
                    "batch_id": "PREV-B1",
                    "case_id": cid,
                    "analysis_digest": self.d1,
                    "decisions": [
                        {"seq": 0, "decision": "resolved",
                         "handled_at": iso(120), "note": "ok"},
                    ],
                },
            )
            self.assertEqual(r.status_code, 200, r.text)

        # 一个绑定"其他摘要"的案件（伪造摘要，直接入库）：不得列入影响面
        with sqlite3.connect(_TMP_DB) as conn:
            import time

            conn.execute(
                """INSERT INTO cases
                   (case_id, batch_id, analysis_digest, content_hash,
                    status, created_at)
                   VALUES ('CASE-STALE', 'PREV-B1', 'stale-digest', 'x',
                           'closed', ?)""",
                (time.time(),),
            )

        # B2 建一个案件（当前 B2 无异常，空 decisions 即 closed）
        r = self.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "PREV-B2",
                "case_id": "CASE-B2",
                "analysis_digest": self.d2,
                "decisions": [],
            },
        )
        self.assertEqual(r.status_code, 200, r.text)

        # 联单 1：同时引用 B1(CASE-A) 与 B2(CASE-B2)
        r = self.client.post(
            "/api/v1/shipments/snapshot",
            json={
                "shipment_id": "SHP-MAIN",
                "batches": [
                    {"batch_id": "PREV-B1", "analysis_digest": self.d1,
                     "case_id": "CASE-A", "handoff_at": iso(2000)},
                    {"batch_id": "PREV-B2", "analysis_digest": self.d2,
                     "case_id": "CASE-B2", "handoff_at": iso(2600)},
                ],
            },
        )
        self.assertEqual(r.status_code, 200, r.text)

        # 联单 2：只引用 B1(CASE-Z)
        r = self.client.post(
            "/api/v1/shipments/snapshot",
            json={
                "shipment_id": "SHP-B1-ONLY",
                "batches": [
                    {"batch_id": "PREV-B1", "analysis_digest": self.d1,
                     "case_id": "CASE-Z", "handoff_at": iso(3000)},
                ],
            },
        )
        self.assertEqual(r.status_code, 200, r.text)

        # 联单 3：只引用 B3（B3 没有案件，无法走正常接口；直接插入一条
        # 引用 CASE-STALE 的联单行用于反查隔离验证——CASE-STALE 属 B1，
        # 因此改为插入 B3 无关联单：手工造 B3 案件 CASE-B3 后走接口）
        r = self.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "PREV-B3",
                "case_id": "CASE-B3",
                "analysis_digest": self.d3,
                "decisions": [],
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        r = self.client.post(
            "/api/v1/shipments/snapshot",
            json={
                "shipment_id": "SHP-B3-ONLY",
                "batches": [
                    {"batch_id": "PREV-B3", "analysis_digest": self.d3,
                     "case_id": "CASE-B3", "handoff_at": iso(3200)},
                ],
            },
        )
        self.assertEqual(r.status_code, 200, r.text)

        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B1", "PREV-B2", "PREV-B3"]),
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        by_id = {b["batch_id"]: b for b in body["batches"]}

        # B1：CASE-STALE 绑定其他摘要，不得出现；其余排序去重
        self.assertEqual(
            by_id["PREV-B1"]["affected_case_ids"], ["CASE-A", "CASE-Z"]
        )
        self.assertEqual(
            by_id["PREV-B1"]["affected_shipment_ids"],
            ["SHP-B1-ONLY", "SHP-MAIN"],
        )
        # B2：仅 CASE-B2 / SHP-MAIN
        self.assertEqual(by_id["PREV-B2"]["affected_case_ids"], ["CASE-B2"])
        self.assertEqual(
            by_id["PREV-B2"]["affected_shipment_ids"], ["SHP-MAIN"]
        )
        # B3：摘要也变（档案内容变），案件与联单同样须列出
        self.assertEqual(by_id["PREV-B3"]["affected_case_ids"], ["CASE-B3"])
        self.assertEqual(
            by_id["PREV-B3"]["affected_shipment_ids"], ["SHP-B3-ONLY"]
        )
        # 并集排序去重
        self.assertEqual(
            body["affected_case_ids"],
            ["CASE-A", "CASE-B2", "CASE-B3", "CASE-Z"],
        )
        self.assertEqual(
            body["affected_shipment_ids"],
            ["SHP-B1-ONLY", "SHP-B3-ONLY", "SHP-MAIN"],
        )

    # ---------- 只读：不修改任何数据 ----------

    def test_04_preview_is_read_only_and_repeatable(self):
        def dump_state():
            with sqlite3.connect(_TMP_DB) as conn:
                tables = {}
                for name in [
                    "batches", "anomalies", "cases", "case_decisions",
                    "shipments", "shipment_batches",
                    "calibration_profiles", "calibration_intervals",
                ]:
                    rows = conn.execute(
                        f"SELECT * FROM {name} ORDER BY 1, 2"
                    ).fetchall()
                    tables[name] = rows
                return tables

        before = dump_state()
        body1 = preview_body(batch_ids=["PREV-B1", "PREV-B2"])
        r1 = self.client.post(
            "/api/v1/calibration-profiles/correction-preview", json=body1
        )
        r2 = self.client.post(
            "/api/v1/calibration-profiles/correction-preview", json=body1
        )
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r1.json(), r2.json())
        after = dump_state()
        self.assertEqual(before, after)

    def test_05_existing_endpoints_and_idempotency_still_work(self):
        # 原档案保持不可变：换内容提交仍 409
        r = self.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(intervals=[
                {"start": iso(0), "end": iso(1800), "scale": 1.0, "offset": 0.9},
            ]),
        )
        self.assertEqual(r.status_code, 409)
        # 批次 GET 回放仍是原档案结果
        r = self.client.get("/api/v1/batches/PREV-B1")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["analysis_digest"], self.d1)
        self.assertEqual(
            r.json()["anomalies"][0]["peak_temperature"], 9.5
        )
        # 同内容幂等 POST：HIT
        r = self.client.post(
            "/api/v1/batches/analyze", json=analysis_body("PREV-B1")
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")
        self.assertEqual(r.json()["analysis_digest"], self.d1)
        # 联单同内容幂等回放仍可用
        r = self.client.post(
            "/api/v1/shipments/snapshot",
            json={
                "shipment_id": "SHP-MAIN",
                "batches": [
                    {"batch_id": "PREV-B1", "analysis_digest": self.d1,
                     "case_id": "CASE-A", "handoff_at": iso(2000)},
                    {"batch_id": "PREV-B2", "analysis_digest": self.d2,
                     "case_id": "CASE-B2", "handoff_at": iso(2600)},
                ],
            },
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")

    # ---------- 拒绝路径 ----------

    def test_06_unknown_profile_rejected_409(self):
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(profile_id="NO-SUCH-PROFILE",
                              batch_ids=["PREV-B1"]),
        )
        self.assertEqual(r.status_code, 409, r.text)
        self.assertIn("NO-SUCH-PROFILE", r.json()["detail"])

    def test_07_missing_batch_rejected_404_and_names_object(self):
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B1", "PREV-GHOST"]),
        )
        self.assertEqual(r.status_code, 404)
        self.assertIn("PREV-GHOST", r.json()["detail"])
        self.assertNotIn("PREV-B1", r.json()["detail"])

    def test_08_batch_of_other_profile_rejected_409(self):
        # 用另一设备档案建档一个批次
        r = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body(
                "PREV-B-OTHER-PROFILE",
                profile_id=PROFILE_OTHER,
                device_id=DEVICE_OTHER,
            ),
        )
        self.assertEqual(r.status_code, 200, r.text)
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B1", "PREV-B-OTHER-PROFILE"]),
        )
        self.assertEqual(r.status_code, 409)
        detail = r.json()["detail"]
        self.assertIn("PREV-B-OTHER-PROFILE", detail)
        self.assertIn(PROFILE_OTHER, detail)

    def test_09_batch_of_other_device_rejected_409(self):
        # 伪造一条同 profile_id 但 device_id 不同的批次行（绕过分析接口）
        import time

        body = analysis_body("PREV-B-FOREIGN-DEV", device_id="reefer-foreign")
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO batches
                   (batch_id, device_id, profile_id, request_hash, request_json,
                    anomaly_count, created_at)
                   VALUES (?, ?, ?, 'h', ?, 0, ?)""",
                ("PREV-B-FOREIGN-DEV", "reefer-foreign", PROFILE,
                 json.dumps(body, ensure_ascii=False, sort_keys=True),
                 time.time()),
            )
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B-FOREIGN-DEV"]),
        )
        self.assertEqual(r.status_code, 409)
        self.assertIn("reefer-foreign", r.json()["detail"])

    def test_10_proposed_intervals_missing_sample_rejected_422(self):
        # B4：含 ts=1200 的采样；拟定区间 [0,1200) 半开不覆盖 1200
        r = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body(
                "PREV-B4",
                samples=[
                    {"timestamp": iso(0), "temperature": 9.0},
                    {"timestamp": iso(1200), "temperature": 9.0},
                ],
            ),
        )
        self.assertEqual(r.status_code, 200, r.text)
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B1", "PREV-B4"]),
        )
        self.assertEqual(r.status_code, 422, r.text)
        detail = r.json()["detail"]
        self.assertIn("PREV-B4", detail)
        self.assertIn(iso_out(1200), detail)

    def test_11_invalid_interval_and_batch_list_shape_rejected_422(self):
        # 空批次清单
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=[]),
        )
        self.assertEqual(r.status_code, 422)
        # 重复批次
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=["PREV-B1", "PREV-B1"]),
        )
        self.assertEqual(r.status_code, 422)
        # 超过 20 个
        r = self.client.post(
            "/api/v1/calibration-profiles/correction-preview",
            json=preview_body(batch_ids=[f"B{i}" for i in range(21)]),
        )
        self.assertEqual(r.status_code, 422)
        # 区间重叠 / end<=start / 非正系数（沿用原建档规则）
        for intervals in [
            [
                {"start": iso(0), "end": iso(700), "scale": 1.0, "offset": 0.6},
                {"start": iso(600), "end": iso(1200), "scale": 1.0, "offset": 0.6},
            ],
            [{"start": iso(600), "end": iso(600), "scale": 1.0, "offset": 0.6}],
            [{"start": iso(0), "end": iso(1200), "scale": 0.0, "offset": 0.6}],
            [{"start": iso(0), "end": iso(1200), "scale": 1.0, "offset": -1.0}],
        ]:
            r = self.client.post(
                "/api/v1/calibration-profiles/correction-preview",
                json=preview_body(intervals=intervals, batch_ids=["PREV-B1"]),
            )
            self.assertEqual(r.status_code, 422, intervals)


if __name__ == "__main__":
    unittest.main()
