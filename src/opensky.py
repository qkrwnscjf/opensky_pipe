import requests
import json
from datetime import datetime

# 대한민국 주변의 위도(lat)와 경도(lon) 범위
# (참고: https://www.latlong.net/country/south-korea/214)
LAT_MIN = 33.0
LAT_MAX = 39.0
LON_MIN = 124.5
LON_MAX = 132.0

# OpenSky Network API URL
# bbox = (위도 최소, 위도 최대, 경도 최소, 경도 최대)
url = f"https://opensky-network.org/api/states/all?lamin={LAT_MIN}&lomin={LON_MIN}&lamax={LAT_MAX}&lomax={LON_MAX}"

try:
    # API 호출
    response = requests.get(url)
    response.raise_for_status() # 오류가 있으면 예외 발생

    # JSON 데이터 파싱
    data = response.json()

    print(f"--- {datetime.now()} 기준, 대한민국 상공 항공기 정보 ---")

    if data['states']:
        for state in data['states']:
            # OpenSky 데이터 필드 인덱스:
            # 0: icao24 (고유 ID)
            # 1: callsign (콜사인, 항공편명)
            # 5: longitude (경도)
            # 6: latitude (위도)
            # 7: baro_altitude (고도)

            callsign = state[1].strip() if state[1] else "N/A"
            longitude = state[5]
            latitude = state[6]
            altitude = state[7]

            print(f"✈️ 콜사인: {callsign:<10} | 위도: {latitude:<10} | 경도: {longitude:<10} | 고도: {altitude}m")
    else:
        print("현재 감지된 항공기가 없습니다.")

except requests.exceptions.RequestException as e:
    print(f"API 호출 중 오류 발생: {e}")
except json.JSONDecodeError:
    print("데이터 파싱 중 오류 발생: 유효하지 않은 JSON 형식입니다.")
except Exception as e:
    print(f"알 수 없는 오류 발생: {e}")