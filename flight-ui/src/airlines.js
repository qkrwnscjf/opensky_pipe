// 항공기 국가 판정 — 편명 앞 3글자(ICAO 항공사 코드)로 "어느 나라 항공사인가"를 정한다.
//
// OpenSky의 origin_country는 출발국이 아니라 기체 "등록국"(icao24 주소 대역으로 판정)이다.
// 그래서 도쿄발 대한항공도, 인천발 홍콩 캐세이퍼시픽(등록 대역이 중국)도 등록국으로만 보인다.
// 편명이 표에 있는 항공사 코드면 그 항공사의 국가를, 아니면(편명 없음 N/A, 개인기 등록부호
// HL8xxx·PHxxx, 표에 없는 코드) 등록국을 그대로 쓴다 — 틀린 국가를 지어내기보다 등록국이 낫다.
//
// 표는 이 공역에서 실제 관측된 코드(2026-10-03 flight_data) 중심으로, 확실한 것만 넣었다.
// 국가 이름은 OpenSky 표기("Republic of Korea", "Viet Nam" 등)에 맞춰 등록국과 섞여도 한 줄로 모이게 했다.

export const AIRLINES = {
  // 대한민국
  KAL: ['Korean Air', 'Republic of Korea'],
  AAR: ['Asiana Airlines', 'Republic of Korea'],
  JJA: ['Jeju Air', 'Republic of Korea'],
  JNA: ['Jin Air', 'Republic of Korea'],
  TWB: ["T'way Air", 'Republic of Korea'],
  ABL: ['Air Busan', 'Republic of Korea'],
  ASV: ['Air Seoul', 'Republic of Korea'],
  ESR: ['Eastar Jet', 'Republic of Korea'],
  APZ: ['Air Premia', 'Republic of Korea'],
  EOK: ['Aero K', 'Republic of Korea'],
  // 중국 본토
  CCA: ['Air China', 'China'],
  CES: ['China Eastern', 'China'],
  CSN: ['China Southern', 'China'],
  CXA: ['Xiamen Airlines', 'China'],
  CSC: ['Sichuan Airlines', 'China'],
  CSZ: ['Shenzhen Airlines', 'China'],
  CSH: ['Shanghai Airlines', 'China'],
  CDG: ['Shandong Airlines', 'China'],
  CHH: ['Hainan Airlines', 'China'],
  CQH: ['Spring Airlines', 'China'],
  DKH: ['Juneyao Air', 'China'],
  CAO: ['Air China Cargo', 'China'],
  CKK: ['China Cargo Airlines', 'China'],
  // 홍콩 — 기체 등록 대역은 중국이라 등록국으로는 구분되지 않는다
  CPA: ['Cathay Pacific', 'Hong Kong'],
  HKE: ['HK Express', 'Hong Kong'],
  CRK: ['Hong Kong Airlines', 'Hong Kong'],
  // 대만
  CAL: ['China Airlines', 'Taiwan'],
  EVA: ['EVA Air', 'Taiwan'],
  SJX: ['Starlux Airlines', 'Taiwan'],
  TTW: ['Tigerair Taiwan', 'Taiwan'],
  // 일본
  JAL: ['Japan Airlines', 'Japan'],
  ANA: ['All Nippon Airways', 'Japan'],
  APJ: ['Peach Aviation', 'Japan'],
  JJP: ['Jetstar Japan', 'Japan'],
  SKY: ['Skymark Airlines', 'Japan'],
  TZP: ['ZIPAIR Tokyo', 'Japan'],
  NCA: ['Nippon Cargo Airlines', 'Japan'],
  JTA: ['Japan Transocean Air', 'Japan'],
  // 동남아·남아시아
  PAL: ['Philippine Airlines', 'Philippines'],
  CEB: ['Cebu Pacific', 'Philippines'],
  HVN: ['Vietnam Airlines', 'Viet Nam'],
  VJC: ['VietJet Air', 'Viet Nam'],
  SIA: ['Singapore Airlines', 'Singapore'],
  THA: ['Thai Airways', 'Thailand'],
  MAS: ['Malaysia Airlines', 'Malaysia'],
  GIA: ['Garuda Indonesia', 'Indonesia'],
  AIC: ['Air India', 'India'],
  MGL: ['MIAT Mongolian Airlines', 'Mongolia'],
  // 중동·아프리카
  UAE: ['Emirates', 'United Arab Emirates'],
  ETD: ['Etihad Airways', 'United Arab Emirates'],
  QTR: ['Qatar Airways', 'Qatar'],
  THY: ['Turkish Airlines', 'Turkey'],
  ETH: ['Ethiopian Airlines', 'Ethiopia'],
  // 유럽
  AFR: ['Air France', 'France'],
  DLH: ['Lufthansa', 'Germany'],
  BAW: ['British Airways', 'United Kingdom'],
  KLM: ['KLM Royal Dutch Airlines', 'Kingdom of the Netherlands'],
  FIN: ['Finnair', 'Finland'],
  AFL: ['Aeroflot', 'Russian Federation'],
  // 미주
  UAL: ['United Airlines', 'United States'],
  AAL: ['American Airlines', 'United States'],
  DAL: ['Delta Air Lines', 'United States'],
  ASA: ['Alaska Airlines', 'United States'],
  FDX: ['FedEx Express', 'United States'],
  UPS: ['UPS Airlines', 'United States'],
  GTI: ['Atlas Air', 'United States'],
  ACA: ['Air Canada', 'Canada'],
};

// 국기 이미지(flagcdn)용 ISO 3166-1 alpha-2 코드. 항공사 국가와 등록국을 모두 덮는다.
export const COUNTRY_CODE = {
  'Republic of Korea': 'kr',
  'South Korea': 'kr',
  China: 'cn',
  'Hong Kong': 'hk',
  Taiwan: 'tw',
  Japan: 'jp',
  Philippines: 'ph',
  'Viet Nam': 'vn',
  Singapore: 'sg',
  Thailand: 'th',
  Malaysia: 'my',
  Indonesia: 'id',
  India: 'in',
  Mongolia: 'mn',
  'United Arab Emirates': 'ae',
  Qatar: 'qa',
  Turkey: 'tr',
  Ethiopia: 'et',
  France: 'fr',
  Germany: 'de',
  'United Kingdom': 'gb',
  'Kingdom of the Netherlands': 'nl',
  Netherlands: 'nl',
  Finland: 'fi',
  Sweden: 'se',
  'Russian Federation': 'ru',
  'United States': 'us',
  Canada: 'ca',
};

// 편명 앞 3글자가 항공사 코드면 그 항공사, 아니면 등록국으로 되돌린다.
// country_source로 어느 쪽에서 왔는지 남겨 화면에서 구분해 보여 줄 수 있게 한다.
export function resolveCountry(flight) {
  const prefix = (flight.callsign || '').trim().slice(0, 3).toUpperCase();
  const hit = AIRLINES[prefix];
  if (hit) {
    return { airline: hit[0], country: hit[1], country_source: 'airline' };
  }
  return { airline: null, country: flight.origin_country || null, country_source: 'registration' };
}
