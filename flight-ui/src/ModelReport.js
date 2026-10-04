import React, { useEffect, useMemo, useState } from 'react';

// Model Report — src/ml_train_trajectory.py가 내보낸 정적 파일(ml_runs.json)만 읽는다.
// 백엔드를 거치지 않는다(사용자 결정 2026-10-04, 정적 파일 방식). 실행 기록의 원본은 MinIO의
// ml/trajectory_next_position/<run_id>/metrics.json이고, 이 파일은 학습 때 다시 만들어지는 사본이다.
const REPORT_URL = '/ml-report/ml_runs.json';
// 같은 조건 비교(기준선·HGB·Chronos-Bolt tiny) — src/ml_compare_models.py가 ML 전용 컨테이너에서 내보낸다
const COMPARE_URL = '/ml-report/ml_compare.json';

// 특성 이름 → 화면 설명. 이름은 ml_train_trajectory.py의 FEATURES와 같아야 한다.
const FEATURE_LABELS = {
  latitude: 'Latitude',
  longitude: 'Longitude',
  baro_altitude: 'Barometric altitude',
  geo_altitude: 'GPS altitude',
  velocity: 'Reported ground speed',
  track_sin: 'Heading (sin)',
  track_cos: 'Heading (cos)',
  vertical_rate: 'Climb / descent rate',
  dt_s: 'Seconds since previous report',
  calc_speed_mps: 'Speed computed from last segment',
  heading_change_deg: 'Heading change since previous report',
  alt_change_m: 'Altitude change since previous report',
  velocity_change_mps: 'Speed change since previous report',
  next_dt_s: 'Seconds until next report',
  dr_north_m: 'Dead-reckoning move, north',
  dr_east_m: 'Dead-reckoning move, east',
};

// 2026-10-04 이전 실행의 metrics.json에는 모델 사양이 없다. 그때 코드(ml_train_trajectory.py)의
// 설정을 그대로 옮긴 값으로 보여 주고, 사양이 기록된 실행은 그 기록을 쓴다.
const MODEL_SPEC_FALLBACK = {
  library: 'scikit-learn 1.3.2',
  estimator: 'HistGradientBoostingRegressor',
  targets: ['north_m', 'east_m'],
  params: {
    max_iter: 300,
    learning_rate: 0.05,
    max_leaf_nodes: 31,
    min_samples_leaf: 20,
    l2_regularization: 0,
    early_stopping: 'auto',
    validation_fraction: 0.1,
    n_iter_no_change: 10,
    random_state: 42,
  },
  n_iter: null,
  split: 'GroupShuffleSplit by segment_id, test_size=0.2, random_state=42',
};

// 조기 종료 'auto'는 학습 행이 1만 개를 넘으면 켜진다(scikit-learn 규칙)
const earlyStoppingNote = (spec, trainRows) => {
  const es = spec.params?.early_stopping;
  if (es === true) return 'on';
  if (es === false) return 'off';
  if (es === 'auto') return trainRows > 10000 ? `auto → on (${trainRows.toLocaleString()} train rows > 10,000)` : 'auto → off (≤ 10,000 train rows)';
  return '—';
};

function ModelSpec({ run }) {
  const spec = run.model || MODEL_SPEC_FALLBACK;
  const p = spec.params || {};
  const iters = spec.n_iter
    ? `north ${spec.n_iter.north} · east ${spec.n_iter.east} (of max ${p.max_iter})`
    : `up to ${p.max_iter} — actual count not recorded for this run`;
  const rows = [
    ['Algorithm', `${spec.estimator} (histogram-based gradient boosting trees)`],
    ['Library', spec.library],
    ['Models', `2 independent regressors — one per target (${(spec.targets || []).join(', ')})`],
    ['Learning rate', p.learning_rate],
    ['Boosting iterations', iters],
    ['Tree size', `max ${p.max_leaf_nodes} leaves · min ${p.min_samples_leaf} samples per leaf`],
    ['L2 regularization', p.l2_regularization],
    ['Early stopping', `${earlyStoppingNote(spec, run.train_rows ?? 0)} · ${Math.round((p.validation_fraction ?? 0) * 100)}% validation, patience ${p.n_iter_no_change}`],
    ['Missing values', 'Handled natively (no imputation) — first point of a segment keeps its NaN deltas'],
    ['Train / test split', spec.split],
    ['Random seed', p.random_state],
    ['Baseline', 'Dead reckoning — hold current speed and heading for the time until the next report'],
  ];
  return (
    <div className="mr-spec">
      <table className="mr-table mr-spec-table">
        <tbody>
          {rows.map(([k, v]) => (
            <tr key={k}>
              <th scope="row">{k}</th>
              <td>{String(v)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {!run.model && (
        <div className="mr-meta">
          This run predates spec recording, so the values come from the training code at that time. Runs from 2026-10-04 on store their own spec.
        </div>
      )}
    </div>
  );
}

const METRICS = [
  { key: 'p50_m', label: 'Median (p50)', hint: 'Half of predictions land within this distance' },
  { key: 'p90_m', label: 'p90', hint: 'How bad the worst 10% get' },
  { key: 'mean_m', label: 'Mean', hint: 'Pulled up by large misses' },
];

const SERIES = [
  { key: 'baseline_dead_reckoning', label: 'Baseline · dead reckoning', cls: 'baseline' },
  { key: 'model_hist_gradient_boosting', label: 'Model · gradient boosting', cls: 'model' },
];

// 비교 섹션의 시리즈 — 색은 엔터티를 따른다(기준선 주황, HGB 파랑은 위와 같고 Chronos만 청록 추가)
const CMP_SERIES = [
  { key: 'baseline_dead_reckoning', label: 'Baseline · dead reckoning', cls: 'baseline' },
  { key: 'hgb', label: 'HGB · gradient boosting', cls: 'model' },
  { key: 'chronos_bolt_tiny', label: 'Chronos-Bolt tiny', cls: 'chronos' },
];

const KST = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Seoul',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
});

// run_id = "YYYYMMDDTHHMMSSZ-xxxxxx" (UTC) → Date
const runDate = (runId) => {
  const m = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/.exec(runId || '');
  return m ? new Date(Date.UTC(+m[1], m[2] - 1, +m[3], +m[4], +m[5], +m[6])) : null;
};
const fmtRunTime = (runId) => {
  const d = runDate(runId);
  return d ? `${KST.format(d)} KST` : runId;
};
const fmtInt = (n) => (typeof n === 'number' ? n.toLocaleString() : '—');
const fmtM = (n) => (typeof n === 'number' ? `${n.toFixed(1)} m` : '—');

function verdictOf(run) {
  const b = run.baseline_dead_reckoning?.p50_m;
  const m = run.model_hist_gradient_boosting?.p50_m;
  if (typeof b !== 'number' || typeof m !== 'number') return null;
  const diff = m - b;
  return {
    modelWins: diff < 0,
    diff: Math.abs(diff),
    pct: b > 0 ? (Math.abs(diff) / b) * 100 : 0,
    baseline: b,
    model: m,
  };
}

// 오차 비교: 지표 3개 × 시리즈 2개를 하나의 0 기준 축(미터)으로 그린다.
function ErrorBars({ run, series = SERIES }) {
  const [hover, setHover] = useState(null);
  const max = Math.max(
    ...METRICS.flatMap((mt) => series.map((s) => run[s.key]?.[mt.key] ?? 0)),
    1
  );
  return (
    <div className="mr-bars" onMouseLeave={() => setHover(null)}>
      {METRICS.map((mt) => (
        <div className="mr-bar-group" key={mt.key}>
          <div className="mr-bar-group-label">
            <b>{mt.label}</b>
            <span>{mt.hint}</span>
          </div>
          {series.map((s) => {
            const v = run[s.key]?.[mt.key];
            const active = hover && hover.metric === mt.key && hover.series === s.key;
            return (
              <div
                className={`mr-bar-row ${hover && !active ? 'dim' : ''}`}
                key={s.key}
                onMouseEnter={() => setHover({ metric: mt.key, series: s.key })}
              >
                <div className="mr-bar-track">
                  <div
                    className={`mr-bar ${s.cls}`}
                    style={{ width: `${((v ?? 0) / max) * 100}%` }}
                  />
                  <span className="mr-bar-value">{fmtM(v)}</span>
                </div>
                {active && (
                  <div className="mr-tip" role="tooltip">
                    <span className={`mr-key ${s.cls}`} />
                    {s.label} — {mt.label}: <b>{fmtM(v)}</b>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

// 실행 기록 추이: p50 두 시리즈. 실행이 2개 이상일 때만 그린다.
function HistoryChart({ runs }) {
  const [hover, setHover] = useState(null);
  const W = 640;
  const H = 220;
  const pad = { l: 48, r: 16, t: 16, b: 28 };
  const values = runs.flatMap((r) => SERIES.map((s) => r[s.key]?.p50_m ?? 0));
  const yMax = Math.max(...values, 1) * 1.15;
  const x = (i) => pad.l + (i * (W - pad.l - pad.r)) / Math.max(runs.length - 1, 1);
  const y = (v) => H - pad.b - (v / yMax) * (H - pad.t - pad.b);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => Math.round(yMax * t));

  return (
    <div className="mr-history-chart">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="p50 error per run, baseline versus model">
        {ticks.map((t) => (
          <g key={t}>
            <line className="mr-grid" x1={pad.l} x2={W - pad.r} y1={y(t)} y2={y(t)} />
            <text className="mr-axis" x={pad.l - 8} y={y(t) + 4} textAnchor="end">{t}</text>
          </g>
        ))}
        {SERIES.map((s) => (
          <polyline
            key={s.key}
            className={`mr-line ${s.cls}`}
            points={runs.map((r, i) => `${x(i)},${y(r[s.key]?.p50_m ?? 0)}`).join(' ')}
          />
        ))}
        {hover !== null && (
          <line className="mr-crosshair" x1={x(hover)} x2={x(hover)} y1={pad.t} y2={H - pad.b} />
        )}
        {SERIES.map((s) =>
          runs.map((r, i) => (
            <circle
              key={`${s.key}-${i}`}
              className={`mr-dot ${s.cls}`}
              cx={x(i)}
              cy={y(r[s.key]?.p50_m ?? 0)}
              r={4.5}
            />
          ))
        )}
        {runs.map((r, i) => (
          <rect
            key={r.run_id}
            className="mr-hit"
            x={x(i) - 18}
            y={pad.t}
            width={36}
            height={H - pad.t - pad.b}
            onMouseEnter={() => setHover(i)}
            onMouseLeave={() => setHover(null)}
          />
        ))}
        <text className="mr-axis" x={pad.l} y={H - 8}>oldest</text>
        <text className="mr-axis" x={W - pad.r} y={H - 8} textAnchor="end">latest</text>
      </svg>
      {hover !== null && (
        <div className="mr-tip mr-tip-chart" style={{ left: `${(x(hover) / W) * 100}%` }}>
          <div className="mr-tip-title">{fmtRunTime(runs[hover].run_id)}</div>
          {SERIES.map((s) => (
            <div key={s.key}>
              <span className={`mr-key ${s.cls}`} /> {s.label}: <b>{fmtM(runs[hover][s.key]?.p50_m)}</b>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// 정확도 표 — 적중률(오차가 기준 거리 이내인 비율)이 주 지표, 상대 정확도는 보조.
// 열마다 가장 좋은 값을 굵게 한다. 기록이 없는 실행(2026-10-04 이전)은 표 대신 안내를 보여 준다.
const HIT_KEYS = ['50', '100', '200'];

function AccuracyTable({ run, series = SERIES }) {
  const rows = series.filter((s) => run[s.key]?.hit_rate_pct);
  if (rows.length === 0) {
    return <div className="mr-meta">Accuracy was not recorded for this run (runs from 2026-10-04 on record it).</div>;
  }
  const best = (get) => Math.max(...rows.map((s) => get(run[s.key]) ?? -Infinity));
  const bestHit = Object.fromEntries(HIT_KEYS.map((k) => [k, best((r) => r.hit_rate_pct?.[k])]));
  const bestRel = best((r) => r.relative_accuracy_p50_pct);
  const pct = (v) => (typeof v === 'number' ? `${v.toFixed(1)}%` : '—');
  return (
    <div className="mr-table-wrap">
      <table className="mr-table mr-acc-table">
        <thead>
          <tr>
            <th>Method</th>
            {HIT_KEYS.map((k) => (
              <th className="num" key={k}>Within {k} m</th>
            ))}
            <th className="num">Relative accuracy</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((s) => {
            const r = run[s.key];
            return (
              <tr key={s.key}>
                <td><span className={`mr-key ${s.cls}`} /> {s.label}</td>
                {HIT_KEYS.map((k) => (
                  <td className={`num mono ${r.hit_rate_pct?.[k] === bestHit[k] && rows.length > 1 ? 'best' : ''}`} key={k}>
                    {pct(r.hit_rate_pct?.[k])}
                  </td>
                ))}
                <td className={`num mono ${r.relative_accuracy_p50_pct === bestRel && rows.length > 1 ? 'best' : ''}`}>
                  {pct(r.relative_accuracy_p50_pct)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function AccuracyNotes() {
  return (
    <>
      <p className="mr-card-sub mr-acc-note">
        <b>Within X m</b> = share of predictions whose error is at most X metres (the main accuracy measure).{' '}
        <b>Relative accuracy</b> = 1 − median(error ÷ actual distance moved); it runs high because aircraft move far,
        so differences between methods look small there. Best value per column in bold.
      </p>
      <p className="mr-ko">
        회귀 문제라 "맞음/틀림"이 없어, 오차가 50·100·200 m 이내인 예측의 비율(적중률)을 정확도로 씁니다.
        상대 정확도는 실제 이동 거리 대비 오차로 계산해 높게 나오므로 보조 지표로만 보세요.
      </p>
    </>
  );
}

function Legend({ series = SERIES }) {
  return (
    <div className="mr-legend">
      {series.map((s) => (
        <span key={s.key}>
          <span className={`mr-key ${s.cls}`} /> {s.label}
        </span>
      ))}
    </div>
  );
}

function Comparison({ report }) {
  const runs = report.runs;
  const run = runs[runs.length - 1];
  const res = run.results || {};
  const ranked = CMP_SERIES.filter((s) => typeof res[s.key]?.p50_m === 'number')
    .sort((a, b) => res[a.key].p50_m - res[b.key].p50_m);
  const best = ranked[0];
  // 중앙값 1위와 나쁜 경우(p90) 1위가 다를 수 있다 — 첫 비교에서 실제로 달랐다(기준선 vs HGB)
  const bestP90 = CMP_SERIES.filter((s) => typeof res[s.key]?.p90_m === 'number')
    .sort((a, b) => res[a.key].p90_m - res[b.key].p90_m)[0];
  const base = res.baseline_dead_reckoning?.p50_m;
  const ch = run.models?.chronos_bolt_tiny || {};
  const hgb = run.models?.hgb || {};
  const fr = run.framing || {};
  const ko = {
    baseline_dead_reckoning: '직진 가정(기준선)',
    hgb: 'HGB',
    chronos_bolt_tiny: 'Chronos-Bolt tiny',
  };

  return (
    <section className="mr-card">
      <div className="mr-card-head">
        <h2>Head-to-head · {fr.horizon_s ?? 10} s ahead, same conditions</h2>
        <Legend series={CMP_SERIES} />
      </div>
      <p className="mr-card-sub">
        All three methods predict the position {fr.horizon_s ?? 10} seconds after each position report, scored on the same
        held-out flights. Inputs are resampled to a {fr.step_s ?? 10} s grid that ends at the report, so no future data leaks in.
        These numbers are not comparable with the next-report errors above — the task is different.
      </p>
      <p className="mr-ko">
        세 방법이 같은 문제(각 위치 보고 {fr.horizon_s ?? 10}초 뒤 위치)를 같은 평가 비행으로 풉니다.
        {best && typeof base === 'number' && ` 가장 정확한 것은 ${ko[best.key]}(중앙값 ${res[best.key].p50_m.toFixed(1)} m)입니다.`}
        {bestP90 && best && bestP90.key !== best.key &&
          ` 다만 크게 빗나가는 경우(p90)는 ${ko[bestP90.key]}가 가장 작습니다(${res[bestP90.key].p90_m.toFixed(1)} m).`}
        {' '}위쪽 "다음 보고 시점" 결과와는 문제가 달라 숫자를 직접 비교하지 않습니다.
      </p>

      {best && (
        <div className="mr-rank">
          {ranked.map((s, i) => (
            <div className={`mr-rank-item ${i === 0 ? 'top' : ''}`} key={s.key}>
              <span className="mr-rank-no mono">{i + 1}</span>
              <span className={`mr-key ${s.cls}`} />
              <span className="mr-rank-name">{s.label}</span>
              <b className="mono">{fmtM(res[s.key].p50_m)}</b>
              {s.key !== 'baseline_dead_reckoning' && typeof base === 'number' && (
                <span className="mr-rank-delta">
                  {res[s.key].p50_m < base
                    ? `${(((base - res[s.key].p50_m) / base) * 100).toFixed(1)}% better than baseline`
                    : `${(((res[s.key].p50_m - base) / base) * 100).toFixed(1)}% worse than baseline`}
                </span>
              )}
            </div>
          ))}
        </div>
      )}

      <ErrorBars run={res} series={CMP_SERIES} />

      <h3 className="mr-sub-h">Accuracy</h3>
      <AccuracyTable run={res} series={CMP_SERIES} />
      <AccuracyNotes />

      <h3 className="mr-sub-h">The three methods</h3>
      <p className="mr-ko">
        Chronos-Bolt tiny는 Amazon이 공개한 무료(Apache-2.0) 시계열 사전학습 모델로, 우리 데이터로 학습하지 않고 그대로 예측합니다.
        CPU에서 돌며, 아래에 실제 추론 시간을 적었습니다.
      </p>
      <div className="mr-models">
        <div className="mr-model">
          <div className="mr-model-head"><span className="mr-key baseline" /> Baseline</div>
          <dl>
            <dt>Method</dt><dd>Dead reckoning — hold reported speed and heading</dd>
            <dt>Training</dt><dd>None (physics)</dd>
          </dl>
        </div>
        <div className="mr-model">
          <div className="mr-model-head"><span className="mr-key model" /> HGB</div>
          <dl>
            <dt>Model</dt><dd>{hgb.estimator || 'HistGradientBoostingRegressor'} × 2 (north, east)</dd>
            <dt>Library</dt><dd>{hgb.library || '—'}</dd>
            <dt>Training</dt><dd>Trained on {fmtInt(run.train_samples)} samples from this lake</dd>
            <dt>Iterations</dt><dd>{hgb.n_iter ? `north ${hgb.n_iter.north} · east ${hgb.n_iter.east} (max ${hgb.params?.max_iter})` : '—'}</dd>
            <dt>Inputs</dt><dd>{(hgb.features || []).length} — last 3 moves, speed, heading, altitude, climb rate, changes, dead-reckoning move</dd>
          </dl>
        </div>
        <div className="mr-model">
          <div className="mr-model-head"><span className="mr-key chronos" /> Chronos-Bolt tiny</div>
          <dl>
            <dt>Model</dt><dd className="mono">{ch.model_id || 'amazon/chronos-bolt-tiny'}</dd>
            <dt>Licence</dt><dd>Apache-2.0 (free, open weights)</dd>
            <dt>Size</dt><dd>{typeof ch.parameters === 'number' ? `${(ch.parameters / 1e6).toFixed(1)} M parameters` : '—'}</dd>
            <dt>Training</dt><dd>{ch.mode || 'zero-shot'}</dd>
            <dt>Inputs</dt><dd>{ch.input || '—'}</dd>
            <dt>Runtime</dt><dd>{ch.device ? `${ch.device.toUpperCase()} · ${ch.inference_ms_per_series} ms per series · ${ch.inference_s} s total (${fmtInt(ch.series_predicted)} series, ${ch.threads} threads)` : '—'}</dd>
            <dt>Software</dt><dd>{[ch.package, ch.torch && `torch ${ch.torch}`].filter(Boolean).join(' · ') || '—'}</dd>
          </dl>
        </div>
      </div>

      <h3 className="mr-sub-h">Conditions</h3>
      <div className="mr-kpis">
        <div className="mr-kpi">
          <div className="mr-kpi-value mono">{run.event_dates?.length ?? '—'}</div>
          <div className="mr-kpi-label">Days of data</div>
          <div className="mr-kpi-note">{(run.event_dates || []).join(', ')}</div>
        </div>
        <div className="mr-kpi">
          <div className="mr-kpi-value mono">{fmtInt(run.segments)}</div>
          <div className="mr-kpi-label">Flights (segments)</div>
          <div className="mr-kpi-note">Train/test overlap {fmtInt(run.segment_overlap)}{run.segment_overlap === 0 ? ' ✓' : ' ⚠'}</div>
        </div>
        <div className="mr-kpi">
          <div className="mr-kpi-value mono">{fmtInt(run.test_samples)}</div>
          <div className="mr-kpi-label">Test samples (all 3 methods)</div>
          <div className="mr-kpi-note">
            {run.test_samples_before_cap > run.test_samples
              ? `Random ${fmtInt(run.test_samples)} of ${fmtInt(run.test_samples_before_cap)} (seed 42) to bound CPU time`
              : `of ${fmtInt(run.samples)} samples`}
          </div>
        </div>
        <div className="mr-kpi">
          <div className="mr-kpi-value mono">{fr.context_max_steps ?? '—'}</div>
          <div className="mr-kpi-label">Max input steps</div>
          <div className="mr-kpi-note">{fr.step_s} s grid, min {fr.context_min_steps} steps; gaps &gt; {fr.max_raw_gap_s} s split the series</div>
        </div>
      </div>
      <div className="mr-meta">
        Run {fmtRunTime(run.run_id)} · <span className="mono">{run.run_id}</span> · {runs.length} comparison run{runs.length === 1 ? '' : 's'} ·
        source <span className="mono">{report.model_prefix}/&lt;run_id&gt;/metrics.json</span>
      </div>
    </section>
  );
}

export default function ModelReport() {
  const [state, setState] = useState({ status: 'loading', report: null });
  const [compare, setCompare] = useState(null);

  useEffect(() => {
    let cancelled = false;
    fetch(COMPARE_URL, { cache: 'no-store' })
      .then((res) => (res.ok ? res.json() : null))
      .then((r) => {
        if (!cancelled && r && Array.isArray(r.runs) && r.runs.length > 0) setCompare(r);
      })
      .catch(() => {}); // 비교 실행 전이면 섹션만 생략한다
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    fetch(REPORT_URL, { cache: 'no-store' })
      .then((res) => {
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((report) => {
        if (cancelled) return;
        setState(
          report && Array.isArray(report.runs) && report.runs.length > 0
            ? { status: 'ok', report }
            : { status: 'missing', report: null }
        );
      })
      .catch(() => {
        // CRA 개발 서버는 없는 파일에 index.html(200)을 돌려주므로 JSON 파싱 실패도 "없음"으로 본다
        if (!cancelled) setState({ status: 'missing', report: null });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const runs = useMemo(() => state.report?.runs ?? [], [state.report]);
  const latest = runs[runs.length - 1];
  const verdict = latest ? verdictOf(latest) : null;

  return (
    <main className="model-report">
      <div className="mr-head">
        <div className="hero-eyebrow">Machine learning</div>
        <h1>Model Report</h1>
        <p>
          Can a model predict where an aircraft will be at its next position report better than
          simple physics? Each training run is scored against a dead-reckoning baseline on flights
          it never saw. The pipeline is built and verified, not operated.
        </p>
        <p className="mr-ko">
          항공기의 다음 위치 보고 지점을 모델이 단순 물리 계산(현재 속도·방향 유지)보다 잘 맞히는지 평가한 보고서입니다.
          학습에 쓰지 않은 비행으로만 채점하며, 모델은 구축·검증만 하고 운용하지 않습니다.
        </p>
      </div>

      {state.status === 'loading' && (
        <div className="mr-card mr-empty">
          <div className="overlay-spinner" />
          Loading report…
        </div>
      )}

      {state.status === 'missing' && (
        <div className="mr-card mr-empty">
          <div className="overlay-title">No report yet</div>
          <div className="overlay-desc">
            Train once (or re-export the existing runs) with the stack running:
            <pre className="mr-cmd">docker compose exec airflow python src/ml_train_trajectory.py{'\n'}docker compose exec airflow python src/ml_train_trajectory.py --export-only</pre>
          </div>
        </div>
      )}

      {state.status === 'ok' && latest && (
        <>
          {/* ① 결론 */}
          {verdict && (
            <section className="mr-card mr-verdict">
              <div className={`mr-verdict-badge ${verdict.modelWins ? 'win' : 'lose'}`}>
                {verdict.modelWins ? '▲ Model wins' : '▼ Baseline wins'}
              </div>
              <div className="mr-verdict-figures">
                <div>
                  <div className="mr-hero mono">{verdict.model.toFixed(1)}<span>m</span></div>
                  <div className="mr-hero-label"><span className="mr-key model" /> Model p50 error</div>
                </div>
                <div className="mr-vs">vs</div>
                <div>
                  <div className="mr-hero mono">{verdict.baseline.toFixed(1)}<span>m</span></div>
                  <div className="mr-hero-label"><span className="mr-key baseline" /> Baseline p50 error</div>
                </div>
              </div>
              <p className="mr-verdict-text">
                The model is <b>{verdict.diff.toFixed(1)} m ({verdict.pct.toFixed(1)}%) {verdict.modelWins ? 'more' : 'less'} accurate</b>{' '}
                than holding the current speed and heading. For scale, a cruising airliner covers about 2.5 km in
                10 seconds — the error is measured against that movement.
              </p>
              <p className="mr-ko">
                {verdict.modelWins
                  ? `모델이 기준선보다 오차가 ${verdict.diff.toFixed(1)} m(${verdict.pct.toFixed(1)}%) 작습니다 — 학습한 의미가 있는 결과입니다.`
                  : `모델이 기준선보다 오차가 ${verdict.diff.toFixed(1)} m(${verdict.pct.toFixed(1)}%) 큽니다 — 아직은 직진 가정이 더 정확합니다.`}
              </p>
              <div className="mr-meta">Latest run · {fmtRunTime(latest.run_id)} · <span className="mono">{latest.run_id}</span></div>
            </section>
          )}

          {/* 같은 조건 3개 모델 비교 (있을 때만) */}
          {compare && <Comparison report={compare} />}

          {/* ② 오차 비교 */}
          <section className="mr-card">
            <div className="mr-card-head">
              <h2>Prediction error</h2>
              <Legend />
            </div>
            <p className="mr-card-sub">Distance between predicted and actual next position, on the held-out test flights. Lower is better.</p>
            <p className="mr-ko">예측 위치와 실제 위치 사이의 거리입니다. 짧을수록 좋고, p50은 중앙값, p90은 나쁜 쪽 10% 경계입니다.</p>
            <ErrorBars run={latest} />
            <h3 className="mr-sub-h">Accuracy</h3>
            <AccuracyTable run={latest} />
            <AccuracyNotes />
          </section>

          {/* ③ 신뢰도 */}
          <section className="mr-card">
            <div className="mr-card-head">
              <h2>Can this result be trusted?</h2>
            </div>
            <p className="mr-ko">
              데이터 일수와 비행 수가 적으면 결과가 쉽게 흔들립니다. 학습·평가에 같은 비행이 섞이지 않아야(겹침 0) 점수를 믿을 수 있습니다.
            </p>
            <div className="mr-kpis">
              <div className="mr-kpi">
                <div className="mr-kpi-value mono">{latest.event_dates?.length ?? '—'}</div>
                <div className="mr-kpi-label">Days of data</div>
                <div className="mr-kpi-note">{(latest.event_dates || []).map((d) => String(d).slice(0, 10)).join(', ')}</div>
              </div>
              <div className="mr-kpi">
                <div className="mr-kpi-value mono">{fmtInt(latest.segments)}</div>
                <div className="mr-kpi-label">Flights (segments)</div>
                <div className="mr-kpi-note">Few flights → weaker generalisation</div>
              </div>
              <div className="mr-kpi">
                <div className="mr-kpi-value mono">{fmtInt(latest.test_rows)}</div>
                <div className="mr-kpi-label">Test rows</div>
                <div className="mr-kpi-note">of {fmtInt(latest.rows)} · train {fmtInt(latest.train_rows)}</div>
              </div>
              <div className="mr-kpi">
                <div className="mr-kpi-value mono">{fmtInt(latest.segment_overlap)}</div>
                <div className="mr-kpi-label">Train/test flight overlap</div>
                <div className="mr-kpi-note">
                  {latest.segment_overlap === 0 ? '✓ No leakage — split by flight' : '⚠ Same flight in both sets'}
                </div>
              </div>
              <div className="mr-kpi">
                <div className="mr-kpi-value mono">{latest.max_next_dt_s ?? '—'}<span>s</span></div>
                <div className="mr-kpi-label">Max time to next report</div>
                <div className="mr-kpi-note">Longer gaps are signal loss, excluded</div>
              </div>
            </div>
          </section>

          {/* ④ 모델 설명 */}
          <section className="mr-card">
            <div className="mr-card-head">
              <h2>What the model sees</h2>
            </div>
            <p className="mr-card-sub">
              Target: displacement to the next report in metres north and east. Two gradient-boosting regressors
              (one per axis), {latest.features?.length ?? 0} input features from the Gold trajectory table.
            </p>
            <p className="mr-ko">
              다음 보고까지 북쪽·동쪽으로 몇 m 움직일지를 예측합니다. 속도·방향·고도와 직전 구간 변화량 등 {latest.features?.length ?? 0}개 값을 입력으로 씁니다.
            </p>
            <h3 className="mr-sub-h">Model</h3>
            <p className="mr-ko">
              scikit-learn의 HistGradientBoostingRegressor(히스토그램 기반 그래디언트 부스팅 트리)를 북쪽·동쪽 방향에 하나씩, 모두 2개 학습했습니다.
              결측값은 그대로 처리하고, 비행 단위로 80:20으로 나눠 평가합니다.
            </p>
            <ModelSpec run={latest} />
            <h3 className="mr-sub-h">Input features</h3>
            <div className="mr-features">
              {(latest.features || []).map((f) => (
                <span className="mr-feature" key={f} title={f}>
                  {FEATURE_LABELS[f] || f}
                </span>
              ))}
            </div>
          </section>

          {/* ⑤ 실행 기록 */}
          <section className="mr-card">
            <div className="mr-card-head">
              <h2>Run history</h2>
              {runs.length >= 2 && <Legend />}
            </div>
            {runs.length >= 2 ? (
              <HistoryChart runs={runs} />
            ) : (
              <p className="mr-card-sub">The trend chart appears after a second run. One run so far.</p>
            )}
            <p className="mr-ko">학습할 때마다 한 줄씩 쌓입니다. 데이터가 늘수록 모델이 기준선을 따라잡는지 확인하는 곳입니다.</p>
            <div className="mr-table-wrap">
              <table className="mr-table">
                <thead>
                  <tr>
                    <th>Run (KST)</th>
                    <th>Days</th>
                    <th>Flights</th>
                    <th className="num">Baseline p50</th>
                    <th className="num">Model p50</th>
                    <th>Result</th>
                  </tr>
                </thead>
                <tbody>
                  {[...runs].reverse().map((r) => {
                    const v = verdictOf(r);
                    return (
                      <tr key={r.run_id}>
                        <td title={r.run_id}>{fmtRunTime(r.run_id)}</td>
                        <td>{r.event_dates?.length ?? '—'}</td>
                        <td>{fmtInt(r.segments)}</td>
                        <td className="num mono">{fmtM(r.baseline_dead_reckoning?.p50_m)}</td>
                        <td className="num mono">{fmtM(r.model_hist_gradient_boosting?.p50_m)}</td>
                        <td>{v ? (v.modelWins ? '▲ Model' : '▼ Baseline') : '—'}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <div className="mr-meta">
              Report generated {state.report.generated_at ? fmtRunTime(state.report.generated_at.replace(/[-:]/g, '').replace(/\.\d+/, '')) : '—'} ·
              source <span className="mono">{state.report.model_prefix}/&lt;run_id&gt;/metrics.json</span>
            </div>
          </section>
        </>
      )}
    </main>
  );
}
