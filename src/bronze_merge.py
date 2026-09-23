"""Bronze 일별 파일 병합. 하루치 작은 Parquet 파일들을 소수의 파일로 합친다. (0-6f)

  spark-submit ... src/bronze_merge.py --date 2026-09-22

【왜 필요한가】

콜드 스트리밍은 트리거마다 파일을 하나 쓴다. 60초 트리거(2026-09-23)에서는 주간
하루 약 1,440개, 야간에는 파일 하나가 몇 KB다. Bronze는 영구 보관이라 이 파일들이
그대로 누적된다. Silver가 하루치를 Iceberg로 다시 쓰긴 하지만 Bronze 원본은 남는다.

【무엇을 하고, 무엇을 하지 않는가】

- 한다: 파일 개수만 줄인다. 같은 행을 (icao24, timestamp) 순으로 정렬해 다시 쓴다.
- 하지 않는다: 행을 빼거나 바꾸지 않는다. **중복도 그대로 둔다.** Bronze는 받은 그대로
  두고 중복 제거는 Silver가 맡는다는 역할 분담을 지킨다(사용자 결정, 09-23).
  압축 코덱도 기존과 같은 snappy다.

정렬하는 이유: 같은 기체의 값이 몰려 압축이 잘 되고, Parquet의 min/max 통계가 좁아져
나중에 icao24로 거를 때 읽지 않고 건너뛰는 부분이 늘어난다. 물리적 순서만 바뀌고
내용은 같다.

【지키는 불변 조건】

1. 원본은 병합본이 자리를 잡기 전까지 지우지 않는다. "병합본 추가 → 원본 삭제"만 허용.
   반대 순서면 데이터가 비는 구간이 생긴다.
2. 처음에 찍은 목록의 파일만 읽고, 그 파일만 지운다. 목록을 찍은 뒤 늦게 들어온
   파일(크래시 후 재처리 등)은 건드리지 않고 남긴다.
3. 지우기 전에 내용이 같은지 검증한다 — 행 수 + 전체 행 해시 합.
4. 어느 단계에서 죽어도 다음 실행이 이어서 끝낸다. 이를 위해 원본을 지우기 전에
   manifest(무엇을 지워야 하는지)를 먼저 남긴다.

【확정 시점】

병합본이 라이브 경로(positions/dt=D/)에 **전부** 복사된 순간. 그 전에는 원본이 기준이고,
그 뒤에는 병합본이 기준이며 manifest의 원본 목록은 지워야 할 쓰레기다.

확정 후 원본 삭제가 끝나기 전까지는 원본과 병합본이 함께 있어 중복 행이 보인다. 지난
날짜의 Bronze를 읽는 곳은 Silver뿐이고 Silver는 (icao24, timestamp)로 중복을 제거하므로,
병합본(원본과 완전히 같은 행)은 그대로 걸러진다. 이 구간은 무해하다.

S3에는 원자적 교체(rename)가 없어서 이 순서를 직접 짠다.
"""

import argparse
import json
import math
import os
import sys
import uuid

from botocore.exceptions import ClientError
from pyspark.sql import functions as F

from lakehouse_common import LAKE_BUCKET, build_spark, s3_client, validate_date, write_marker

# DAG의 BRONZE_PREFIX와 같은 값이어야 한다. boto3는 접두사, Spark는 s3a:// 경로로 쓴다.
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "positions")
# 임시 작업 공간. positions/ 밖에 둬야 Silver가 절대 읽지 않는다.
STAGING_PREFIX = os.getenv("BRONZE_MERGE_STAGING", "bronze_merge_staging")
MERGE_MARKER_PREFIX = os.getenv("BRONZE_MERGE_MARKER_PREFIX", "etl_markers_bronze_merge")

# 병합본 파일 하나의 **최대** 크기. 하루에 쌓이는 양이 아니다.
# 하루치가 이보다 작으면 파일 1개, 크면 여러 개로 나눈다. 지금 규모(하루 ~1MB, 24시간
# 가동해도 ~34MB)에서는 항상 1개다. B-1처럼 하루치가 커졌을 때 파일 하나가 수 GB로
# 불어나지 않게 막는 안전장치다.
TARGET_FILE_BYTES = int(os.getenv("BRONZE_MERGE_TARGET_BYTES", str(128 * 1024 * 1024)))

# 병합본 이름. "_"나 "."로 시작하면 Spark/Hadoop이 숨김 파일로 보고 읽지 않는다.
MERGED_NAME_PREFIX = "merged-"


def s3a(key):
    return f"s3a://{LAKE_BUCKET}/{key}"


def list_parquet(client, prefix):
    """prefix 바로 아래의 데이터 파일(.parquet)만. 숨김 파일(_SUCCESS 등)은 뺀다."""
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=LAKE_BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            leaf = o["Key"].rsplit("/", 1)[-1]
            if leaf.endswith(".parquet") and not leaf.startswith(("_", ".")):
                out.append({"Key": o["Key"], "Size": o["Size"]})
    return out


def exists(client, key):
    try:
        client.head_object(Bucket=LAKE_BUCKET, Key=key)
        return True
    except ClientError:
        return False


def delete_keys(client, keys):
    """없는 키를 지워도 S3는 오류를 내지 않는다 — 재시도가 안전하다."""
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        if chunk:
            client.delete_objects(
                Bucket=LAKE_BUCKET, Delete={"Objects": [{"Key": k} for k in chunk]}
            )


def delete_prefix(client, prefix):
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=LAKE_BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    delete_keys(client, keys)
    return len(keys)


def read_manifest(client, key):
    if not exists(client, key):
        return None
    body = client.get_object(Bucket=LAKE_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def fingerprint(df, cols):
    """(행 수, 전체 행 해시 합).

    행 수만 비교하면 "개수는 같은데 값이 바뀐" 경우를 못 잡는다. 행마다 xxhash64를
    구해 더하면 행의 순서와 무관하고(정렬해도 같음), 중복 행도 그 수만큼 반영된다.
    long 합은 넘칠 수 있어 decimal(38,0)로 더한다.
    """
    r = df.select(cols).agg(
        F.count("*").alias("n"),
        F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h"),
    ).first()
    return r["n"], str(r["h"])


def finish(client, date_str, manifest, stage_root):
    """확정 이후 단계: 남은 원본 삭제 → 임시 공간 정리 → 마커.

    정상 경로와 복구 경로가 모두 여기로 온다. 각 단계가 다시 실행돼도 안전하다.
    """
    delete_keys(client, manifest["originals"])
    delete_prefix(client, stage_root)  # manifest를 포함해 마지막에 지운다
    write_marker(
        MERGE_MARKER_PREFIX, date_str, manifest["rows"],
        f"{len(manifest['originals'])} files/{manifest['in_bytes']}B -> "
        f"{len(manifest['merged'])} files/{manifest['out_bytes']}B",
    )
    print(
        f"MERGE_DONE: date={date_str} files {len(manifest['originals'])} -> "
        f"{len(manifest['merged'])} bytes {manifest['in_bytes']} -> {manifest['out_bytes']} "
        f"rows={manifest['rows']}"
    )


def main():
    parser = argparse.ArgumentParser(description="Bronze 일별 파일 병합")
    parser.add_argument("--date", required=True, help="병합할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()
    validate_date(args.date)

    date_str = args.date
    client = s3_client()
    live_prefix = f"{BRONZE_PREFIX}/dt={date_str}/"
    stage_root = f"{STAGING_PREFIX}/{date_str}/"
    manifest_key = stage_root + "manifest.json"
    print(f"MERGE_START: date={date_str} live={live_prefix}")

    # ── 0. 이전 실행이 남긴 흔적 처리 ─────────────────────────────
    manifest = read_manifest(client, manifest_key)
    if manifest is not None:
        finals = [m["final"] for m in manifest["merged"]]
        if all(exists(client, k) for k in finals):
            # 확정 후에 죽었다. 병합본이 기준이므로 남은 원본만 지우고 마무리한다.
            print(f"MERGE_RECOVERY: 확정 후 중단 발견 — 남은 원본 삭제로 마무리 (run={manifest['run_id']})")
            finish(client, date_str, manifest, stage_root)
            return
        # 확정 전에 죽었다. 원본은 온전하다. 일부만 복사된 병합본이 라이브 경로에 남아
        # 있으면 다음 목록에 원본으로 섞여 행이 두 번 들어가므로 반드시 먼저 지운다.
        partial = [k for k in finals if exists(client, k)]
        delete_keys(client, partial)
        print(f"MERGE_RECOVERY: 확정 전 중단 — 부분 복사본 {len(partial)}개 삭제 후 처음부터")
    # manifest 이전 단계에서 죽었으면 임시 파일만 남아 있다.
    delete_prefix(client, stage_root)

    # ── 1. 원본 목록 찍기 ─────────────────────────────────────
    originals = list_parquet(client, live_prefix)
    if len(originals) <= 1:
        # 이미 1개 이하 — 할 일이 없다. 이전에 병합이 끝났거나 원래 파일이 하나다.
        # 행 수는 세지 않는다(Spark를 띄울 이유가 없음) — 마커의 rows=-1은 "안 셈"이다.
        write_marker(MERGE_MARKER_PREFIX, date_str, -1, f"{len(originals)} file(s), nothing to merge")
        print(f"MERGE_SKIP: date={date_str} files={len(originals)}")
        return
    in_bytes = sum(o["Size"] for o in originals)
    n_files = max(1, math.ceil(in_bytes / TARGET_FILE_BYTES))
    print(f"MERGE_PLAN: files={len(originals)} bytes={in_bytes} -> target_files={n_files}")

    spark = build_spark(f"bronze_merge_{date_str}")
    spark.sparkContext.setLogLevel("WARN")
    try:
        # ── 2. 목록의 파일만 읽기 ─────────────────────────────
        # 디렉터리가 아니라 파일을 하나하나 지정한다(불변 조건 2).
        src = spark.read.parquet(*[s3a(o["Key"]) for o in originals])
        # 파일 안에는 dt가 없다(경로에만 있음). 경로 추론으로 생기더라도 빼서 양쪽을 맞춘다.
        cols = [c for c in src.columns if c != "dt"]
        n_in, h_in = fingerprint(src, cols)
        print(f"MERGE_READ: rows={n_in} hash={h_in}")

        # ── 3. 임시 경로에 병합본 쓰기 ──────────────────────────
        # repartitionByRange → 파일끼리도 (icao24, timestamp) 범위로 나뉘고,
        # sortWithinPartitions → 파일 안도 정렬된다. n_files=1이면 전체 정렬.
        run_id = uuid.uuid4().hex[:8]
        stage_data = f"{stage_root}run_{run_id}/"
        (
            src.select(cols)
            .repartitionByRange(n_files, "icao24", "timestamp")
            .sortWithinPartitions("icao24", "timestamp")
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(s3a(stage_data))
        )

        # ── 4. 검증 ────────────────────────────────────────────
        staged = list_parquet(client, stage_data)
        merged_df = spark.read.parquet(*[s3a(o["Key"]) for o in staged])
        n_out, h_out = fingerprint(merged_df, cols)
        if (n_in, h_in) != (n_out, h_out):
            delete_prefix(client, stage_root)
            print(f"MERGE_VERIFY_FAILED: in=({n_in},{h_in}) out=({n_out},{h_out}) — 원본은 그대로 둔다")
            sys.exit(1)
        out_bytes = sum(o["Size"] for o in staged)
        print(f"MERGE_VERIFIED: rows={n_out} hash match, files={len(staged)} bytes={out_bytes}")
    finally:
        spark.stop()

    # ── 5. manifest — 원본을 건드리기 전에 "무엇을 지울지"를 먼저 남긴다 ──
    merged = [
        {"staged": o["Key"], "final": f"{live_prefix}{MERGED_NAME_PREFIX}{date_str}-{run_id}-{i:03d}.snappy.parquet"}
        for i, o in enumerate(staged)
    ]
    manifest = {
        "date": date_str, "run_id": run_id, "rows": n_in, "hash": h_in,
        "originals": [o["Key"] for o in originals], "merged": merged,
        "in_bytes": in_bytes, "out_bytes": out_bytes,
    }
    client.put_object(Bucket=LAKE_BUCKET, Key=manifest_key, Body=json.dumps(manifest).encode("utf-8"))

    # ── 6. 확정 — 병합본을 라이브 경로로 복사 (서버 측 복사) ────────
    for m in merged:
        client.copy_object(
            Bucket=LAKE_BUCKET, CopySource={"Bucket": LAKE_BUCKET, "Key": m["staged"]}, Key=m["final"]
        )
    print(f"MERGE_COMMITTED: {len(merged)} file(s) in {live_prefix}")

    # ── 7~9. 원본 삭제 → 정리 → 마커 ─────────────────────────
    finish(client, date_str, manifest, stage_root)


if __name__ == "__main__":
    main()
