"""재처리 도우미 (1-13, 2026-10-12) — 단계·날짜를 지정해 완료 마커를 지우고, 늦게 들어온 Bronze 파일을 찾는다.

DAG는 "Bronze에 있는 날짜 − 마커가 있는 날짜"를 처리하므로, 마커를 지우는 것이 곧 "다음 실행에서 다시 처리"다.
예전에는 MinIO 콘솔이나 mc로 마커 경로를 손으로 지웠다 — 단계마다 접두사가 다르고(etl_markers/, etl_markers_gold/,
etl_markers_bronze_merge/), Silver를 다시 하면 Gold D와 D+1(다음 날 Gold가 D를 맥락으로 읽음)도 다시 해야 한다는
연쇄를 기억해야 했다.

    python src/lake_reprocess.py --stage silver --date 2026-10-03 --cascade          # 미리보기(기본)
    python src/lake_reprocess.py --stage silver --date 2026-10-03 --cascade --apply  # 실제로 지움
    python src/lake_reprocess.py --late-check --date 2026-10-03                     # 늦은 파일 찾기
    python src/lake_reprocess.py --stale --stage gold                               # 옛 코드로 처리된 날짜 (1-16)

안전장치:
  - 기본은 미리보기. --apply가 있어야 지운다.
  - `status=EXCLUDED…` 줄이 있는 마커(사용자가 일부러 처리에서 뺀 날짜, 예: 2026-09-22)는 --force 없이는 지우지 않는다.
  - Spark를 띄우지 않는다(boto3만). DAG 실행 중에 지워도 그 실행은 이미 날짜 목록을 정했으므로 다음 실행부터 반영된다.

늦은 파일 판정: positions/dt=D/ 아래 데이터 파일 중 Silver 마커보다 나중에 생긴 것. 병합 파일(merged-*)은 Silver 마커
뒤에 생기는 게 정상이라 뺀다(CLAUDE.md "Reading the cold path").
"""
import argparse
import os
from datetime import datetime, timedelta

from lakehouse_common import LAKE_BUCKET, s3_client, stage_code_version, validate_date

PREFIXES = {
    "silver": os.getenv("ETL_MARKER_PREFIX", "etl_markers"),
    "gold": os.getenv("GOLD_MARKER_PREFIX", "etl_markers_gold"),
    "merge": os.getenv("BRONZE_MERGE_MARKER_PREFIX", "etl_markers_bronze_merge"),
}
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "positions")
MERGED_NAME_PREFIX = "merged-"


def is_excluded(body):
    """일부러 처리에서 뺀 날짜의 마커인가. 실제 형식은 `status=EXCLUDED (not processed)` 줄이다(2026-09-30 작성).
    처음 구현은 본문이 'EXCLUDED'로 시작하는지만 봐서 이 마커를 보호하지 못했다(1-16 점검 중 발견, 10-12)."""
    return any(line.startswith("status=EXCLUDED") or line.startswith("EXCLUDED") for line in body.splitlines())


def marker_key(stage, date_str):
    return f"{PREFIXES[stage]}/dt={date_str}/_SUCCESS"


def next_day(date_str):
    return (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def head(client, key):
    try:
        return client.head_object(Bucket=LAKE_BUCKET, Key=key)
    except client.exceptions.ClientError:
        return None


def plan(stages, dates, cascade):
    """지울 (단계, 날짜) 목록. --cascade: Silver D → Gold D, D+1까지."""
    targets = []
    for d in dates:
        for s in stages:
            targets.append((s, d))
            if cascade and s == "silver":
                targets += [("gold", d), ("gold", next_day(d))]
    seen, out = set(), []
    for t in targets:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def reprocess(client, targets, apply, force):
    for stage, d in targets:
        key = marker_key(stage, d)
        meta = head(client, key)
        if meta is None:
            print(f"REPROCESS_ABSENT: {stage} {d} (마커 없음 — 이미 다음 실행 대상)")
            continue
        body = client.get_object(Bucket=LAKE_BUCKET, Key=key)["Body"].read().decode("utf-8", "replace")
        if is_excluded(body) and not force:
            print(f"REPROCESS_PROTECTED: {stage} {d} (EXCLUDED 마커 — 지우려면 --force)")
            continue
        if apply:
            client.delete_object(Bucket=LAKE_BUCKET, Key=key)
            print(f"REPROCESS_DELETED: {stage} {d}")
        else:
            print(f"REPROCESS_WOULD_DELETE: {stage} {d}")


def late_check(client, dates):
    for d in dates:
        marker = head(client, marker_key("silver", d))
        if marker is None:
            print(f"LATE_CHECK: {d} Silver 마커 없음 — 아직 처리 전이라 늦은 파일 개념이 없음")
            continue
        done_at = marker["LastModified"]
        late, total = [], 0
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=LAKE_BUCKET, Prefix=f"{BRONZE_PREFIX}/dt={d}/"):
            for obj in page.get("Contents", []):
                leaf = obj["Key"].rsplit("/", 1)[-1]
                if not leaf.endswith(".parquet") or leaf.startswith(("_", ".")):
                    continue
                total += 1
                if not leaf.startswith(MERGED_NAME_PREFIX) and obj["LastModified"] > done_at:
                    late.append(leaf)
        print(f"LATE_CHECK: {d} files={total} late={len(late)} silver_done_at={done_at.isoformat()}")
        for leaf in late[:20]:
            print(f"  LATE_FILE: {leaf}")


def marker_dates(client, stage):
    dates = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=LAKE_BUCKET, Prefix=f"{PREFIXES[stage]}/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if parts[-1] == "_SUCCESS" and parts[-2].startswith("dt="):
                dates.append(parts[-2][len("dt="):])
    return sorted(dates)


def stale(client, stages, dates):
    """1-16: 마커의 code_version이 지금 코드와 다른 날짜(없으면 'unknown' — 1-16 이전에 처리됨). 지우지 않고 보여 주기만
    한다. EXCLUDED 마커는 일부러 뺀 날짜라 목록에서 따로 표시한다. 다시 처리하려면 --stage … --date … --apply."""
    for stage in stages:
        current = stage_code_version(stage)
        found = dates or marker_dates(client, stage)
        stale_dates, unknown, excluded = [], [], []
        for d in found:
            meta = head(client, marker_key(stage, d))
            if meta is None:
                continue
            body = client.get_object(Bucket=LAKE_BUCKET, Key=marker_key(stage, d))["Body"].read().decode("utf-8", "replace")
            if is_excluded(body):
                excluded.append(d)
                continue
            version = next((line.split("=", 1)[1] for line in body.splitlines() if line.startswith("code_version=")), None)
            if version is None:
                unknown.append(d)
            elif version != current:
                stale_dates.append(d)
        print(f"STALE_CHECK: {stage} current={current} markers={len(found)} stale={len(stale_dates)} "
              f"unknown={len(unknown)} excluded={len(excluded)}")
        for label, items in (("STALE", stale_dates), ("UNKNOWN", unknown)):
            if items:
                print(f"  {label}_DATES: {' '.join(items)}")


def main():
    parser = argparse.ArgumentParser(description="레이크하우스 재처리 도우미 (마커 삭제 / 늦은 파일 점검)")
    parser.add_argument("--date", nargs="*", default=[], help="날짜 (YYYY-MM-DD), 여러 개 가능 (--stale은 생략 시 전체)")
    parser.add_argument("--stage", nargs="+", choices=sorted(PREFIXES), help="마커를 지울 단계")
    parser.add_argument("--cascade", action="store_true", help="Silver를 지우면 Gold D·D+1도 함께")
    parser.add_argument("--apply", action="store_true", help="실제로 지운다(없으면 미리보기)")
    parser.add_argument("--force", action="store_true", help="EXCLUDED 마커도 지운다")
    parser.add_argument("--late-check", action="store_true", help="Silver 마커 뒤에 들어온 Bronze 파일 찾기")
    parser.add_argument("--stale", action="store_true", help="지금 코드와 다른 버전으로 처리된 날짜 보기 (지우지 않음)")
    args = parser.parse_args()
    for d in args.date:
        validate_date(d)
    if not args.stage and not args.late_check and not args.stale:
        parser.error("--stage, --late-check, --stale 중 하나는 필요하다")
    if not args.date and not args.stale:
        parser.error("--date가 필요하다(--stale만 생략 가능)")

    client = s3_client()
    if args.stale:
        stale(client, args.stage or sorted(PREFIXES), sorted(set(args.date)))
        return
    if args.late_check:
        late_check(client, sorted(set(args.date)))
    if args.stage:
        targets = plan(args.stage, sorted(set(args.date)), args.cascade)
        print(f"REPROCESS_MODE: {'apply' if args.apply else 'dry-run'} targets={len(targets)}")
        reprocess(client, targets, args.apply, args.force)


if __name__ == "__main__":
    main()
