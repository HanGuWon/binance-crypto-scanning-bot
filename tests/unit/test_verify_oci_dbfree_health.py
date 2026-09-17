# ruff: noqa
from __future__ import annotations
import hashlib, json
from pathlib import Path
from tools.verify_oci_dbfree_health import canonical, verify_receipts

def _receipt(path: Path, captured: int, previous: str | None = None) -> str:
    m={"latest_manifest_path":None,"latest_segment_path":None,"latest_partial_path":None,"latest_received_at_ms":None,"latest_manifest_mtime_ms":None,"latest_partial_mtime_ms":None,"freshness_age_ms":None,"partial_freshness_age_ms":None,"manifest_count_delta":0,"finalized_bytes_delta":0,"partial_bytes":0,"actively_advancing":False,"parsed_manifests":0,"new_manifests":0,"malformed_manifests":0,"missing_segments":0,"size_mismatches":0,"hash_mismatches":0,"predecessor_breaks":0,"campaign_mismatches":0,"source_mismatches":0,"observed_source_identity":None,"new_source_identities":[],"all_new_source_match":None,"scan_complete":True,"historical_hashed_segments":0,"historical_hashed_bytes":0,"healthy":True}
    value={"schema":"oci_dbfree_health_receipt_v2","mode":"fast","receipt_id":f"fast-v2-{captured}","captured_at_ms":captured,"campaign_id":"camp","expected_source_identity":"src","observed_source_identity":None,"source_identity_match":None,"source_identity_all_new_match":None,"collector":{"unit":"collector","active_state":"active","sub_state":"running","main_pid":1,"n_restarts":0,"restart_delta":0,"exec_main_start_timestamp":None,"result":"success"},"markets":{"spot":m,"futures":dict(m)},"storage":{"data_free_bytes":1},"journal":{"oom_count":0},"manifest_integrity":{"healthy":True},"growth":{"sample_count":0},"classification":{"state":"YELLOW","reasons":["RUNWAY_UNKNOWN"]},"cursor":{"cursor_schema":"v2.1","captured_at_ms":captured,"journal_cursor_ms":captured,"markets":{},"cumulative_finalized_bytes":0,"partial_sizes":{},"receipt_sequence":captured},"previous_receipt_sha256":previous,"receipt_sha256":None}
    value["receipt_sha256"]=hashlib.sha256(canonical(value)).hexdigest(); (path/f"fast-v2-{captured}.json").write_text(json.dumps(value),encoding="utf-8"); return value["receipt_sha256"]

def test_verifier_accepts_chain_and_rejects_tamper(tmp_path: Path) -> None:
    first=_receipt(tmp_path,100); _receipt(tmp_path,200,first); assert verify_receipts(tmp_path,"fast")==[]
    tampered=tmp_path/"fast-v2-200.json"; value=json.loads(tampered.read_text()); value["classification"]["state"]="GREEN"; tampered.write_text(json.dumps(value)); assert verify_receipts(tmp_path,"fast")

def test_verifier_rejects_mode_and_filename_mismatch(tmp_path: Path) -> None:
    _receipt(tmp_path,100); wrong=tmp_path/"fast-v2-101.json"; wrong.write_text((tmp_path/"fast-v2-100.json").read_text()); assert verify_receipts(tmp_path,"fast")

def test_verifier_rejects_empty_directory(tmp_path: Path) -> None:
    assert verify_receipts(tmp_path,"fast")
