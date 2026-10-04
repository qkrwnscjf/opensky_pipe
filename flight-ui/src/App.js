import React, { useState, useEffect, useMemo, useRef } from 'react';
import { MapContainer, TileLayer, Marker, Polyline, Tooltip, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import './App.css';
import { COUNTRY_CODE, resolveCountry } from './airlines';
import ModelReport from './ModelReport';

// --- Flight Control UI ---

const getFlagUrl = (country) => {
  const code = country && COUNTRY_CODE[country];
  return code ? `https://flagcdn.com/w40/${code}.png` : null;
};

// 시간 표시는 한국 시간(KST)으로 통일한다. 저장은 UTC 그대로다 — 레이크(Bronze dt, DAG 날짜)가 UTC 기준이라 바꾸지 않는다.
// 백엔드는 Postgres의 `timestamp without time zone`(UTC 값)을 오프셋 없이 내려 주므로,
// 그대로 new Date()에 넣으면 브라우저가 현지 시각으로 오해해 9시간 어긋난다. 그래서 UTC로 명시해 읽는다.
const KST_TIME = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Seoul',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});
const parseUtc = (ts) => {
  if (!ts) return null;
  const d = new Date(/[zZ]|[+-]\d{2}:?\d{2}$/.test(ts) ? ts : `${ts}Z`);
  return Number.isNaN(d.getTime()) ? null : d;
};
const formatKst = (date) => (date ? `${KST_TIME.format(date)} KST` : '—');

const DEFAULT_CENTER = [37.5, 127.0];

// 고도대별 색상 — 실제 항공 추적 서비스처럼 순항 고도를 한눈에 구분한다.
const ALTITUDE_BANDS = [
  { key: 'low', label: 'Low · under 3 km', max: 3000, color: '#f59e0b' },
  { key: 'cruise', label: 'Cruise · 3–9 km', max: 9000, color: '#2563eb' },
  { key: 'high', label: 'High · 9 km+', max: Infinity, color: '#06b6d4' },
];

const bandFor = (altitude) =>
  ALTITUDE_BANDS.find((b) => (altitude ?? 0) < b.max) || ALTITUDE_BANDS[ALTITUDE_BANDS.length - 1];

const DEMO_STEPS = [
  { label: 'Search & Discover' },
  { label: 'Track Live' },
  { label: 'Inspect Details' },
  { label: 'Light & Dark' },
];

// mode='fly'는 선택 시 확대 이동, mode='pan'은 Follow 모드에서 줌 유지한 채 따라가기
// 실시간 화면이 숨겨졌다(display:none) 다시 보이면 Leaflet이 크기를 0으로 기억하고 있어
// 타일이 일부만 그려진다. 보이는 순간 크기를 다시 재게 한다.
function MapResizeOnShow({ visible }) {
  const map = useMap();
  useEffect(() => {
    if (!visible) return undefined;
    const id = setTimeout(() => map.invalidateSize(), 0);
    return () => clearTimeout(id);
  }, [visible, map]);
  return null;
}

function MapFocusHandler({ center, mode }) {
  const map = useMap();
  useEffect(() => {
    if (!center) return;
    if (mode === 'pan') {
      map.panTo(center, { animate: true, duration: 0.8 });
    } else {
      map.flyTo(center, 9, { duration: 1.2, easeLinearity: 0.25 });
    }
  }, [center, mode, map]);
  return null;
}

const aircraftIcon = (flight, active, theme) => {
  const color = bandFor(flight.altitude).color;
  const stroke = theme === 'dark' ? '#061018' : '#ffffff';
  const heading = flight.true_track;
  return L.divIcon({
    className: `aircraft-icon ${active ? 'active' : ''}`,
    html: `
      <div class="aircraft-icon-ring"></div>
      <svg width="26" height="26" viewBox="0 0 40 40" style="transform: rotate(${(heading || 0) - 90}deg); filter: drop-shadow(0 0 4px ${color}99);">
        <path d="M20 5 L35 30 L20 25 L5 30 Z" fill="${color}" stroke="${stroke}" stroke-width="1.5" stroke-linejoin="round" />
      </svg>
    `,
    iconSize: [26, 26],
    iconAnchor: [13, 13],
  });
};

// reveals a section with a fade/slide-in transition the first time it scrolls into view
function useReveal() {
  const ref = useRef(null);
  const [visible, setVisible] = useState(typeof IntersectionObserver === 'undefined');
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof IntersectionObserver === 'undefined') return;
    const obs = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVisible(true);
          obs.disconnect();
        }
      },
      // 비율(threshold 0.15)로 걸면 섹션이 화면의 1/0.15배보다 길어지는 순간 영원히 나타나지 않는다
      // — 기체가 110대로 늘자 #fleet 그리드가 그 길이를 넘어 통째로 투명하게 남았다.
      // 그래서 "윗부분이 화면 하단 10% 안쪽으로 들어오면"으로 판정한다(섹션 길이와 무관).
      { threshold: 0, rootMargin: '0px 0px -10% 0px' }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);
  return [ref, visible];
}

// 목표치가 바뀔 때마다 "현재 표시 중인 값"에서 새 값으로 다시 트윈한다.
// (최초 1회만 카운트업하던 기존 동작 → 실시간 push마다 숫자가 굴러가도록)
function useCountUp(target, duration, start) {
  const [value, setValue] = useState(0);
  const fromRef = useRef(0);
  useEffect(() => {
    if (!start) return undefined;
    const from = fromRef.current;
    const delta = target - from;
    if (delta === 0) return undefined;
    let raf;
    const t0 = performance.now();
    const tick = (now) => {
      const progress = Math.min((now - t0) / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      const next = Math.round(from + delta * eased);
      fromRef.current = next;
      setValue(next);
      if (progress < 1) raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [target, duration, start]);
  return value;
}

const readInitialPinned = () => {
  try {
    const raw = window.localStorage.getItem('skystream-pinned');
    return new Set(raw ? JSON.parse(raw) : []);
  } catch (e) {
    return new Set();
  }
};

const readInitialTheme = () => {
  if (typeof window === 'undefined') return 'light';
  try {
    const saved = window.localStorage.getItem('skystream-theme');
    if (saved === 'light' || saved === 'dark') return saved;
    if (typeof window.matchMedia === 'function') {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
  } catch (e) {
    // localStorage unavailable (private mode, test env, etc.) — fall through
  }
  return 'light';
};

function App() {
  const [flights, setFlights] = useState([]);
  const [selectedIcao, setSelectedIcao] = useState(null);
  const [mapCenter, setMapCenter] = useState(null);
  // 화면 전환은 해시로 한다(라우터 의존성 없이): '#/model' = Model Report, 그 외 = 실시간 화면.
  // 실시간 화면은 숨기기만 하고 언마운트하지 않는다 — WebSocket·지도·나타나기 효과 상태를 유지하기 위해.
  const viewFromHash = () => (window.location.hash === '#/model' ? 'model' : 'live');
  const [view, setView] = useState(viewFromHash);
  const viewRef = useRef(view);
  viewRef.current = view;
  const [query, setQuery] = useState('');
  const [countryFilter, setCountryFilter] = useState('all');
  const [sortKey, setSortKey] = useState('icao');
  const [connected, setConnected] = useState(true);
  const [clock, setClock] = useState(new Date());
  const [theme, setTheme] = useState(readInitialTheme);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [pulse, setPulse] = useState(0);
  const [followMode, setFollowMode] = useState(false);
  const [hasLoadedOnce, setHasLoadedOnce] = useState(false);
  const [trail, setTrail] = useState([]);
  const [pinned, setPinned] = useState(readInitialPinned);
  const searchRef = useRef(null);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    try {
      window.localStorage.setItem('skystream-theme', theme);
    } catch (e) {
      // ignore storage failures
    }
  }, [theme]);

  useEffect(() => {
    try {
      window.localStorage.setItem('skystream-pinned', JSON.stringify([...pinned]));
    } catch (e) {
      // ignore storage failures
    }
  }, [pinned]);

  const togglePin = (icao24, e) => {
    e.stopPropagation(); // 카드 선택과 겹치지 않도록
    setPinned((prev) => {
      const next = new Set(prev);
      if (next.has(icao24)) next.delete(icao24);
      else next.add(icao24);
      return next;
    });
  };

  // Phase 2 (docs/EXPANSION_PLAN.md): 10초 폴링 대신 WebSocket 실시간 푸시를 구독.
  // 백엔드가 배치 완료 시점에 push하므로, 연결이 끊기면 3초 뒤 자동 재연결한다.
  useEffect(() => {
    let ws;
    let reconnectTimer;

    const connect = () => {
      ws = new WebSocket('ws://localhost:8000/ws/flights');
      ws.onopen = () => setConnected(true);
      ws.onmessage = (event) => {
        try {
          // 국가는 편명(항공사 코드) 기준으로 판정해 붙인다 — airlines.js 참고
          setFlights(JSON.parse(event.data).map((f) => ({ ...f, ...resolveCountry(f) })));
          setLastUpdate(new Date());
          setHasLoadedOnce(true);
          setPulse((p) => p + 1); // 상단 펄스 바 애니메이션 재시작용
        } catch (e) {
          console.error('FLIGHT_DATA_PARSE_FAILED:', e);
        }
      };
      ws.onerror = () => ws.close();
      ws.onclose = () => {
        setConnected(false);
        reconnectTimer = setTimeout(connect, 3000);
      };
    };

    connect();
    const clockInterval = setInterval(() => setClock(new Date()), 1000);
    return () => {
      clearTimeout(reconnectTimer);
      clearInterval(clockInterval);
      ws?.close();
    };
  }, []);

  const selectedFlight = useMemo(
    () => flights.find((f) => f.icao24 === selectedIcao) || null,
    [flights, selectedIcao]
  );

  // 국가 필터 선택지 — 지금 하늘에 있는 국가만, 많은 순으로
  const countryOptions = useMemo(() => {
    const counts = new Map();
    flights.forEach((f) => {
      const c = f.country || 'Unknown';
      counts.set(c, (counts.get(c) || 0) + 1);
    });
    return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  }, [flights]);

  const filteredFlights = useMemo(() => {
    const q = query.trim().toLowerCase();
    let base = flights;
    if (countryFilter !== 'all') {
      base = base.filter((f) => (f.country || 'Unknown') === countryFilter);
    }
    if (q) {
      base = base.filter(
        (f) =>
          f.callsign?.toLowerCase().includes(q) ||
          f.icao24?.toLowerCase().includes(q) ||
          f.country?.toLowerCase().includes(q) ||
          f.airline?.toLowerCase().includes(q) ||
          f.origin_country?.toLowerCase().includes(q)
      );
    }
    const byCallsign = (a, b) => {
      // 편명 없는 기체(N/A)는 뒤로
      const ca = a.callsign?.trim() || '\uffff';
      const cb = b.callsign?.trim() || '\uffff';
      return ca.localeCompare(cb);
    };
    const comparators = {
      icao: null, // 백엔드가 icao24 순으로 내려 준다
      country: (a, b) => (a.country || '\uffff').localeCompare(b.country || '\uffff') || byCallsign(a, b),
      altitude: (a, b) => (b.altitude ?? -Infinity) - (a.altitude ?? -Infinity),
      callsign: byCallsign,
    };
    const cmp = comparators[sortKey];
    const sorted = cmp ? [...base].sort(cmp) : base;
    if (pinned.size === 0) return sorted;
    // 안정 정렬이므로 고정한 기체만 위로 오고 나머지는 위 정렬 순서를 유지한다
    return [...sorted].sort(
      (a, b) => (pinned.has(b.icao24) ? 1 : 0) - (pinned.has(a.icao24) ? 1 : 0)
    );
  }, [flights, query, pinned, countryFilter, sortKey]);

  const stats = useMemo(() => {
    if (flights.length === 0) return { count: 0, avgAlt: 0, avgSpd: 0, countries: 0 };
    const altSum = flights.reduce((s, f) => s + (f.altitude || 0), 0);
    const spdSum = flights.reduce((s, f) => s + (f.velocity || 0), 0);
    const countries = new Set(flights.map((f) => f.country).filter(Boolean));
    return {
      count: flights.length,
      avgAlt: Math.round(altSum / flights.length),
      avgSpd: Math.round(spdSum / flights.length),
      countries: countries.size,
    };
  }, [flights]);

  const statsReady = flights.length > 0;
  const countCount = useCountUp(stats.count, 900, statsReady);
  const countAlt = useCountUp(stats.avgAlt, 1200, statsReady);
  const countSpd = useCountUp(stats.avgSpd, 1200, statsReady);
  const countCountries = useCountUp(stats.countries, 800, statsReady);

  const handleSelect = (flight) => {
    setSelectedIcao((cur) => (cur === flight.icao24 ? null : flight.icao24));
    setMapCenter([flight.latitude, flight.longitude]);
  };

  // 선택한 기체의 최근 궤적을 불러온다. 이후 위치는 WebSocket push가 올 때마다 이어붙인다.
  useEffect(() => {
    if (!selectedIcao) {
      setTrail([]);
      return undefined;
    }
    let cancelled = false;
    fetch(`http://localhost:8000/flights/${selectedIcao}/trail`)
      .then((r) => (r.ok ? r.json() : []))
      .then((points) => {
        if (!cancelled) setTrail(points.map((p) => [p.latitude, p.longitude]));
      })
      .catch(() => {
        if (!cancelled) setTrail([]); // 궤적은 부가 정보라, 실패해도 지도는 그대로 둔다
      });
    return () => {
      cancelled = true;
    };
  }, [selectedIcao]);

  useEffect(() => {
    if (!selectedFlight) return;
    setTrail((prev) => {
      const last = prev[prev.length - 1];
      const next = [selectedFlight.latitude, selectedFlight.longitude];
      if (last && last[0] === next[0] && last[1] === next[1]) return prev;
      return [...prev, next];
    });
  }, [selectedFlight]);

  // Follow 모드: 선택한 기체가 움직일 때마다 지도가 따라간다.
  useEffect(() => {
    if (!followMode || !selectedFlight) return;
    setMapCenter([selectedFlight.latitude, selectedFlight.longitude]);
  }, [followMode, selectedFlight]);

  // 선택이 풀리면 Follow도 함께 해제 (따라갈 대상이 없으므로)
  useEffect(() => {
    if (!selectedIcao) setFollowMode(false);
  }, [selectedIcao]);

  useEffect(() => {
    const onHash = () => {
      const next = viewFromHash();
      setView(next);
      if (next === 'model') {
        window.scrollTo(0, 0);
        return;
      }
      // Model Report에서 '#airspace' 같은 섹션 링크를 누르면, 실시간 화면이 다시 보인 뒤에 스크롤한다
      const id = window.location.hash.slice(1);
      requestAnimationFrame(() => {
        const el = id && document.getElementById(id);
        if (el) el.scrollIntoView({ behavior: 'smooth' });
      });
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 키보드 단축키: '/' 검색 포커스, Esc 선택 해제
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'Escape') {
        setSelectedIcao(null);
        return;
      }
      const tag = e.target?.tagName;
      if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA' && tag !== 'SELECT' && viewRef.current === 'live') {
        e.preventDefault();
        searchRef.current?.scrollIntoView({ behavior: 'smooth', block: 'center' });
        searchRef.current?.focus();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  // clock이 1초마다 갱신되므로 이 값도 자연스럽게 카운트업된다.
  const secondsAgo = lastUpdate ? Math.max(0, Math.round((clock - lastUpdate) / 1000)) : null;

  const [heroRef, heroVisible] = useReveal();
  const [showcaseRef, showcaseVisible] = useReveal();
  const [mapRef, mapVisible] = useReveal();
  const [fleetRef, fleetVisible] = useReveal();

  const [demoStep, setDemoStep] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setDemoStep((s) => (s + 1) % DEMO_STEPS.length), 3200);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="page">
      {/* WebSocket push가 올 때마다 key가 바뀌며 애니메이션이 재시작된다 */}
      {pulse > 0 && <div className="live-pulse" key={pulse} />}
      <nav className="topnav">
        <a className="brand" href="#top">
          <span className="brand-mark">✈</span>
          <span className="brand-name">SkyStream</span>
        </a>
        <div className="nav-links">
          <a href="#airspace">Airspace</a>
          <a href="#fleet">Fleet</a>
          <a href="#/model" className={`nav-model ${view === 'model' ? 'active' : ''}`}>Model</a>
        </div>
        <div className="nav-right">
          <div className={`status-pill ${connected ? 'live' : 'down'}`}>
            <span className="status-dot" />
            {connected ? 'LIVE' : 'OFFLINE'}
          </div>
          <span
            className="nav-updated mono"
            title="Time since the last WebSocket push"
          >
            {secondsAgo === null ? '—' : `${secondsAgo}s ago`}
          </span>
          <span className="nav-clock mono">{formatKst(clock)}</span>
          <button
            className="theme-toggle"
            onClick={() => setTheme((t) => (t === 'light' ? 'dark' : 'light'))}
            aria-label="Toggle color theme"
            title="Toggle light / dark mode"
          >
            {theme === 'light' ? '🌙' : '☀️'}
          </button>
        </div>
      </nav>

      {view === 'model' && <ModelReport />}

      <div className="live-view" hidden={view !== 'live'}>
      <header id="top" className={`hero ${heroVisible ? 'in-view' : ''}`} ref={heroRef}>
        <div className="hero-glow glow-a" />
        <div className="hero-glow glow-b" />
        <div className="hero-content">
          <div className="hero-eyebrow">Real-time airspace intelligence</div>
          <h1>SkyStream Flight Control</h1>
          <p className="hero-sub">
            SkyStream streams live aircraft telemetry from the OpenSky Network through Kafka and Spark,
            tracking every flight over the Korean peninsula in real time — down to the second.
          </p>
          <div className="hero-stats">
            <div className="stat-card">
              <div className="stat-value">{countCount}</div>
              <div className="stat-label">Active Aircraft</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {countAlt.toLocaleString()}
                <span className="stat-unit">m</span>
              </div>
              <div className="stat-label">Avg. Altitude</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">
                {countSpd}
                <span className="stat-unit">m/s</span>
              </div>
              <div className="stat-label">Avg. Velocity</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">{countCountries}</div>
              <div className="stat-label">Origin Countries</div>
            </div>
          </div>
          <a className="scroll-cue" href="#airspace">
            <span>Scroll to explore</span>
            <span className="scroll-arrow">↓</span>
          </a>
        </div>
      </header>

      <section className={`showcase ${showcaseVisible ? 'in-view' : ''}`} ref={showcaseRef}>
        <div className="section-head">
          <h2>See It In Action</h2>
          <p>A quick walkthrough of the flight-control experience, right inside your browser.</p>
        </div>

        <div className="device-frame">
          <div className="mac-window">
            <div className="mac-titlebar">
              <div className="mac-dots">
                <span className="mac-dot red" />
                <span className="mac-dot yellow" />
                <span className="mac-dot green" />
              </div>
              <div className="mac-urlbar">
                <span className="mac-lock">🔒</span> localhost:3000
              </div>
              <div className="mac-titlebar-spacer" />
            </div>
            <div className="mac-viewport">
                <div className={`demo-panel ${demoStep === 0 ? 'active' : ''}`}>
                  <div className="mock-topbar">
                    <div className="mock-search">
                      <span>🔍</span> AIH3981
                    </div>
                  </div>
                  <div className="mock-list">
                    <div className="mock-row highlight">
                      <span className="mock-flag" />
                      <div className="mock-row-text">
                        <b>AIH3981</b>
                        <small>71bc19 · Republic of Korea</small>
                      </div>
                    </div>
                    <div className="mock-row">
                      <span className="mock-flag" />
                      <div className="mock-row-text">
                        <b>KAL239</b>
                        <small>71c044 · Republic of Korea</small>
                      </div>
                    </div>
                    <div className="mock-row">
                      <span className="mock-flag" />
                      <div className="mock-row-text">
                        <b>CEB187</b>
                        <small>758770 · Philippines</small>
                      </div>
                    </div>
                  </div>
                </div>

                <div className={`demo-panel ${demoStep === 1 ? 'active' : ''}`}>
                  <div className="mock-map">
                    <span className="mock-marker" style={{ top: '28%', left: '38%' }} />
                    <span className="mock-marker" style={{ top: '58%', left: '62%' }} />
                    <span className="mock-marker active" style={{ top: '44%', left: '50%' }} />
                    <span className="mock-marker" style={{ top: '72%', left: '22%' }} />
                    <span className="mock-marker" style={{ top: '20%', left: '70%' }} />
                  </div>
                </div>

                <div className={`demo-panel ${demoStep === 2 ? 'active' : ''}`}>
                  <div className="mock-detail-card">
                    <div className="mock-detail-head">
                      <span className="mock-flag" />
                      <div>
                        <b>AIH3981</b>
                        <small>Republic of Korea</small>
                      </div>
                    </div>
                    <div className="mock-detail-grid">
                      <div>
                        <span>ALT</span>
                        <b>11,255 m</b>
                      </div>
                      <div>
                        <span>SPD</span>
                        <b>270 m/s</b>
                      </div>
                      <div>
                        <span>HDG</span>
                        <b>5°</b>
                      </div>
                      <div>
                        <span>LAT</span>
                        <b>37.512</b>
                      </div>
                    </div>
                  </div>
                </div>

                <div className={`demo-panel ${demoStep === 3 ? 'active' : ''}`}>
                  <div className="mock-theme">
                    <div className="mock-theme-swatch light" />
                    <div className="mock-toggle">
                      <span className="mock-toggle-knob" />
                    </div>
                    <div className="mock-theme-swatch dark" />
                  </div>
                </div>
              </div>
            </div>

          <div className="demo-steps">
            {DEMO_STEPS.map((step, i) => (
              <button
                key={step.label}
                className={`demo-step-btn ${demoStep === i ? 'active' : ''}`}
                onClick={() => setDemoStep(i)}
              >
                <span className="demo-step-index">{i + 1}</span>
                {step.label}
              </button>
            ))}
          </div>
        </div>
      </section>

      <section id="airspace" className={`map-section ${mapVisible ? 'in-view' : ''}`} ref={mapRef}>
        <div className="section-head">
          <h2>Live Airspace</h2>
          <p>Live aircraft positions, pushed over WebSocket the moment each batch lands.</p>
        </div>
        <div className="map-frame">
          <MapContainer
            center={DEFAULT_CENTER}
            zoom={7}
            zoomControl={false}
            style={{ height: '100%', width: '100%' }}
          >
            {/* Esri Gray Canvas: 바탕(Base)과 지명(Reference)이 별도 레이어라 둘을 겹친다.
                키가 필요 없고, 원본 타일은 16단계까지라 그 이상은 확대해서 보여 준다. */}
            <TileLayer
              key={`${theme}-base`}
              attribution='Tiles &copy; Esri &mdash; Esri, HERE, Garmin, &copy; OpenStreetMap contributors'
              url={`https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_${theme === 'dark' ? 'Dark' : 'Light'}_Gray_Base/MapServer/tile/{z}/{y}/{x}`}
              maxNativeZoom={16}
              maxZoom={19}
            />
            <TileLayer
              key={`${theme}-ref`}
              url={`https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_${theme === 'dark' ? 'Dark' : 'Light'}_Gray_Reference/MapServer/tile/{z}/{y}/{x}`}
              maxNativeZoom={16}
              maxZoom={19}
            />
            <MapFocusHandler center={mapCenter} mode={followMode ? 'pan' : 'fly'} />
            <MapResizeOnShow visible={view === 'live'} />
            {trail.length > 1 && (
              <Polyline
                positions={trail}
                pathOptions={{
                  color: bandFor(selectedFlight?.altitude).color,
                  weight: 2.5,
                  opacity: 0.75,
                  dashArray: '5 7',
                }}
              />
            )}
            {flights.map((flight) => (
              <Marker
                key={flight.icao24}
                position={[flight.latitude, flight.longitude]}
                icon={aircraftIcon(flight, selectedIcao === flight.icao24, theme)}
                eventHandlers={{ click: () => handleSelect(flight) }}
              >
                {/* 호버 정보 — flights가 push마다 갱신되므로 띄워 둔 채로도 값이 실시간으로 바뀐다 */}
                <Tooltip direction="top" offset={[0, -14]} className="flight-tip">
                  <div className="tip-head">
                    <span className="tip-band" style={{ background: bandFor(flight.altitude).color }} />
                    <b>{flight.callsign || 'N/A'}</b>
                    <span className="tip-icao">{flight.icao24}</span>
                  </div>
                  <div className="tip-grid">
                    <span>ALT</span><b>{Math.round(flight.altitude ?? 0).toLocaleString()} m</b>
                    <span>SPD</span><b>{Math.round((flight.velocity ?? 0) * 3.6).toLocaleString()} km/h</b>
                    <span>HDG</span><b>{Math.round(flight.true_track ?? 0)}°</b>
                    <span>AIRLINE</span><b>{flight.airline || '—'}</b>
                    <span>COUNTRY</span><b>{flight.country || '—'}</b>
                    <span>SEEN</span><b>{formatKst(parseUtc(flight.timestamp))}</b>
                  </div>
                </Tooltip>
              </Marker>
            ))}
          </MapContainer>

          {/* 지도만 보고 있어도 핵심 수치가 보이도록 하는 HUD */}
          <div className="map-hud">
            <div className="hud-row">
              <span>Aircraft</span>
              <b>{flights.length}</b>
            </div>
            <div className="hud-row">
              <span>Avg. alt</span>
              <b>{stats.avgAlt.toLocaleString()} m</b>
            </div>
            <div className="hud-row">
              <span>Updated</span>
              <b>{secondsAgo === null ? '—' : `${secondsAgo}s ago`}</b>
            </div>
          </div>

          <div className="map-legend">
            {ALTITUDE_BANDS.map((band) => (
              <div className="legend-item" key={band.key}>
                <span className="legend-swatch" style={{ background: band.color }} />
                {band.label}
              </div>
            ))}
          </div>

          {selectedFlight && (
            <button
              className={`follow-btn ${followMode ? 'active' : ''}`}
              onClick={() => setFollowMode((v) => !v)}
              title="Keep the selected aircraft centered as it moves"
            >
              {followMode ? '◉ Following' : '◎ Follow'} {selectedFlight.callsign || selectedFlight.icao24}
            </button>
          )}

          {!connected && (
            <div className="map-overlay">
              <div className="overlay-card">
                <div className="overlay-icon">🔌</div>
                <div className="overlay-title">Backend disconnected</div>
                <div className="overlay-desc">
                  Can’t reach <code>ws://localhost:8000</code>. Start the pipeline with{' '}
                  <code>docker-compose up -d</code>.
                </div>
                <div className="overlay-retry">
                  <span className="retry-dot" /> Reconnecting…
                </div>
              </div>
            </div>
          )}
          {connected && !hasLoadedOnce && (
            <div className="map-overlay subtle">
              <div className="overlay-card">
                <div className="overlay-spinner" />
                <div className="overlay-title">Scanning airspace…</div>
                <div className="overlay-desc">Waiting for the first batch from Spark.</div>
              </div>
            </div>
          )}
        </div>
      </section>

      <section id="fleet" className={`fleet-section ${fleetVisible ? 'in-view' : ''}`} ref={fleetRef}>
        <div className="section-head">
          <h2>Active Fleet</h2>
          <div className="fleet-search">
            <input
              ref={searchRef}
              type="text"
              placeholder="Search callsign, ICAO24, country…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <kbd className="search-kbd" title="Press / to focus search">/</kbd>
            <select
              className="fleet-select"
              value={countryFilter}
              onChange={(e) => setCountryFilter(e.target.value)}
              aria-label="Filter by country"
            >
              <option value="all">All countries</option>
              {countryOptions.map(([c, n]) => (
                <option key={c} value={c}>{`${c} (${n})`}</option>
              ))}
            </select>
            <select
              className="fleet-select"
              value={sortKey}
              onChange={(e) => setSortKey(e.target.value)}
              aria-label="Sort aircraft"
            >
              <option value="icao">Sort: ICAO24</option>
              <option value="country">Sort: Country</option>
              <option value="callsign">Sort: Callsign</option>
              <option value="altitude">Sort: Altitude ↓</option>
            </select>
            {pinned.size > 0 && (
              <span className="fleet-pinned-count" title="Pinned aircraft">★ {pinned.size}</span>
            )}
            <span className="fleet-count">{filteredFlights.length}</span>
          </div>
        </div>

        {!connected && (
          <div className="empty-state offline-state">
            <div className="overlay-icon">🔌</div>
            <div className="overlay-title">Backend disconnected</div>
            <div className="overlay-desc">
              Run <code>docker-compose up -d</code>, then this list fills itself — no refresh needed.
            </div>
            <div className="overlay-retry">
              <span className="retry-dot" /> Reconnecting…
            </div>
          </div>
        )}

        {/* 첫 배치를 기다리는 동안은 문구 대신 카드 모양 스켈레톤을 보여준다 */}
        {connected && !hasLoadedOnce && (
          <div className="fleet-grid">
            {Array.from({ length: 6 }).map((_, i) => (
              <div className="fleet-card skeleton" key={i}>
                <div className="sk-head">
                  <div className="sk-box" />
                  <div className="sk-lines">
                    <div className="sk-line sk-w60" />
                    <div className="sk-line sk-w40" />
                  </div>
                </div>
                <div className="sk-grid">
                  <div className="sk-line" />
                  <div className="sk-line" />
                  <div className="sk-line" />
                  <div className="sk-line" />
                </div>
              </div>
            ))}
          </div>
        )}

        {connected && hasLoadedOnce && filteredFlights.length === 0 && (
          <div className="empty-state">
            {flights.length === 0 ? 'No aircraft in range right now.' : 'No aircraft match your search.'}
          </div>
        )}

        <div className="fleet-grid">
          {filteredFlights.map((flight) => {
            const isActive = selectedIcao === flight.icao24;
            const isPinned = pinned.has(flight.icao24);
            return (
              <div
                key={flight.icao24}
                className={`fleet-card ${isActive ? 'active' : ''} ${isPinned ? 'pinned' : ''}`}
                onClick={() => handleSelect(flight)}
              >
                <span className="fleet-band" style={{ background: bandFor(flight.altitude).color }} />
                <button
                  className={`pin-btn ${isPinned ? 'active' : ''}`}
                  onClick={(e) => togglePin(flight.icao24, e)}
                  title={isPinned ? 'Unpin from top' : 'Pin to top'}
                  aria-label={isPinned ? 'Unpin aircraft' : 'Pin aircraft to top'}
                >
                  {isPinned ? '★' : '☆'}
                </button>
                <div className="fleet-card-top">
                  {getFlagUrl(flight.country) && (
                    <img className="fleet-flag" src={getFlagUrl(flight.country)} alt={flight.country} />
                  )}
                  <div>
                    <div className="callsign">{flight.callsign || 'N/A'}</div>
                    <div className="icao">{flight.icao24}</div>
                  </div>
                </div>
                <div className="fleet-card-details">
                  <div>
                    <span>ALT</span>
                    <b>{Math.round(flight.altitude ?? 0).toLocaleString()} m</b>
                  </div>
                  <div>
                    <span>SPD</span>
                    <b>{(flight.velocity ?? 0).toFixed(0)} m/s</b>
                  </div>
                  <div>
                    <span>HDG</span>
                    <b>{Math.round(flight.true_track ?? 0)}°</b>
                  </div>
                  <div>
                    <span>COUNTRY</span>
                    <b title={flight.country_source === 'airline' ? `Airline: ${flight.airline}` : 'Registration country (no airline match)'}>
                      {flight.country || '—'}
                    </b>
                  </div>
                </div>
                {isActive && selectedFlight && (
                  <div className="fleet-card-expanded">
                    <div>
                      <span>Latitude</span>
                      <b>{selectedFlight.latitude?.toFixed(3)}</b>
                    </div>
                    <div>
                      <span>Longitude</span>
                      <b>{selectedFlight.longitude?.toFixed(3)}</b>
                    </div>
                    <div>
                      <span>Airline</span>
                      <b>{selectedFlight.airline || '—'}</b>
                    </div>
                    <div>
                      <span>Registered</span>
                      <b>{selectedFlight.origin_country || '—'}</b>
                    </div>
                    <div>
                      <span>Last update</span>
                      <b>{formatKst(parseUtc(selectedFlight.timestamp))}</b>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </section>
      </div>

      <footer className="page-footer">
        <span>SkyStream · Lambda-architecture flight pipeline (Kafka · Spark · PostgreSQL · MinIO)</span>
      </footer>
    </div>
  );
}

export default App;
