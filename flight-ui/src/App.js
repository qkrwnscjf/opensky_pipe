import React, { useState, useEffect, useMemo, useRef } from 'react';
import { MapContainer, TileLayer, Marker, useMap } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import './App.css';

// --- Flight Control UI ---

const COUNTRY_CODE = {
  "South Korea": "kr",
  "Republic of Korea": "kr",
  "United States": "us",
  "Japan": "jp",
  "China": "cn",
  "Taiwan": "tw",
  "United Kingdom": "gb",
  "Germany": "de",
  "France": "fr",
  "Canada": "ca",
};

const getFlagUrl = (country) => {
  const code = country && COUNTRY_CODE[country];
  return code ? `https://flagcdn.com/w40/${code}.png` : null;
};

const DEFAULT_CENTER = [37.5, 127.0];

const DEMO_STEPS = [
  { label: 'Search & Discover' },
  { label: 'Track Live' },
  { label: 'Inspect Details' },
  { label: 'Light & Dark' },
];

function MapFocusHandler({ center }) {
  const map = useMap();
  useEffect(() => {
    if (center) {
      map.flyTo(center, 9, { duration: 1.2, easeLinearity: 0.25 });
    }
  }, [center, map]);
  return null;
}

const aircraftIcon = (heading, active, theme) => {
  const color = theme === 'dark' ? (active ? '#22d3ee' : '#38bdf8') : (active ? '#1d4ed8' : '#2563eb');
  const stroke = theme === 'dark' ? '#061018' : '#ffffff';
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
      { threshold: 0.15 }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);
  return [ref, visible];
}

// animates a number counting up to `target` once `start` becomes true
function useCountUp(target, duration, start) {
  const [value, setValue] = useState(0);
  useEffect(() => {
    if (!start) return undefined;
    let raf;
    const t0 = performance.now();
    const tick = (now) => {
      const progress = Math.min((now - t0) / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setValue(Math.round(target * eased));
      if (progress < 1) raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, duration, start]);
  return value;
}

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
  const [query, setQuery] = useState('');
  const [connected, setConnected] = useState(true);
  const [clock, setClock] = useState(new Date());
  const [theme, setTheme] = useState(readInitialTheme);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    try {
      window.localStorage.setItem('skystream-theme', theme);
    } catch (e) {
      // ignore storage failures
    }
  }, [theme]);

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
          setFlights(JSON.parse(event.data));
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

  const filteredFlights = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return flights;
    return flights.filter(
      (f) =>
        f.callsign?.toLowerCase().includes(q) ||
        f.icao24?.toLowerCase().includes(q) ||
        f.origin_country?.toLowerCase().includes(q)
    );
  }, [flights, query]);

  const stats = useMemo(() => {
    if (flights.length === 0) return { count: 0, avgAlt: 0, avgSpd: 0, countries: 0 };
    const altSum = flights.reduce((s, f) => s + (f.altitude || 0), 0);
    const spdSum = flights.reduce((s, f) => s + (f.velocity || 0), 0);
    const countries = new Set(flights.map((f) => f.origin_country).filter(Boolean));
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
      <nav className="topnav">
        <a className="brand" href="#top">
          <span className="brand-mark">✈</span>
          <span className="brand-name">SkyStream</span>
        </a>
        <div className="nav-links">
          <a href="#airspace">Airspace</a>
          <a href="#fleet">Fleet</a>
        </div>
        <div className="nav-right">
          <div className={`status-pill ${connected ? 'live' : 'down'}`}>
            <span className="status-dot" />
            {connected ? 'LIVE' : 'OFFLINE'}
          </div>
          <span className="nav-clock mono">{clock.toLocaleTimeString('en-GB')}</span>
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
          <div className="macbook">
            <div className="macbook-lid">
              <div className="macbook-camera" />
              <div className="macbook-screen">
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
            <div className="macbook-hinge" />
            <div className="macbook-keyboard-deck">
              <div className="macbook-keys">
                {[0, 1, 2].map((row) => (
                  <div className="key-row" key={row}>
                    {row < 2
                      ? Array.from({ length: 12 }).map((_, i) => <span className="key" key={i} />)
                      : <span className="key key-space" />}
                  </div>
                ))}
              </div>
              <div className="macbook-trackpad" />
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
          <p>Live aircraft positions, refreshed every 10 seconds.</p>
        </div>
        <div className="map-frame">
          <MapContainer
            center={DEFAULT_CENTER}
            zoom={7}
            zoomControl={false}
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer
              key={theme}
              attribution='&copy; CARTO'
              url={
                theme === 'dark'
                  ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
                  : 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'
              }
            />
            <MapFocusHandler center={mapCenter} />
            {flights.map((flight) => (
              <Marker
                key={flight.icao24}
                position={[flight.latitude, flight.longitude]}
                icon={aircraftIcon(flight.true_track, selectedIcao === flight.icao24, theme)}
                eventHandlers={{ click: () => handleSelect(flight) }}
              />
            ))}
          </MapContainer>
          {flights.length === 0 && <div className="map-empty">Scanning airspace…</div>}
        </div>
      </section>

      <section id="fleet" className={`fleet-section ${fleetVisible ? 'in-view' : ''}`} ref={fleetRef}>
        <div className="section-head">
          <h2>Active Fleet</h2>
          <div className="fleet-search">
            <input
              type="text"
              placeholder="Search callsign, ICAO24, country…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <span className="fleet-count">{filteredFlights.length}</span>
          </div>
        </div>

        {flights.length === 0 && <div className="empty-state">Scanning airspace…</div>}
        {flights.length > 0 && filteredFlights.length === 0 && (
          <div className="empty-state">No aircraft match your search.</div>
        )}

        <div className="fleet-grid">
          {filteredFlights.map((flight) => {
            const isActive = selectedIcao === flight.icao24;
            return (
              <div
                key={flight.icao24}
                className={`fleet-card ${isActive ? 'active' : ''}`}
                onClick={() => handleSelect(flight)}
              >
                <div className="fleet-card-top">
                  {getFlagUrl(flight.origin_country) && (
                    <img className="fleet-flag" src={getFlagUrl(flight.origin_country)} alt={flight.origin_country} />
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
                    <span>ORIGIN</span>
                    <b>{flight.origin_country || '—'}</b>
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
                      <span>Last update</span>
                      <b>{new Date(selectedFlight.timestamp).toLocaleTimeString()}</b>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </section>

      <footer className="page-footer">
        <span>SkyStream · Lambda-architecture flight pipeline (Kafka · Spark · PostgreSQL · MinIO)</span>
      </footer>
    </div>
  );
}

export default App;
