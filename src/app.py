import os
import streamlit as st
import pandas as pd
import psycopg2
import time

# 페이지 설정
st.set_page_config(page_title="실시간 항공기 추적", page_icon="✈️", layout="wide")

# DB 연결 함수
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        
        # DB 이름
        database=os.getenv("DB_NAME", "flightdb"),
        
        user=os.getenv("DB_USER", "myuser"),

        password=os.getenv("DB_PASSWORD", "mypassword")
    )

# 데이터 로드 함수
def load_data():
    conn = get_db_connection()
    # 1. 전체 데이터 개수
    count_query = "SELECT COUNT(*) FROM flight_realtime;"
    total_count = pd.read_sql(count_query, conn).iloc[0, 0]
    
    # 2. 최신 데이터 100개
    data_query = "SELECT * FROM flight_realtime ORDER BY timestamp DESC LIMIT 100;"
    df = pd.read_sql(data_query, conn)
    
    conn.close()
    return total_count, df

# 타이틀
st.title("Real-time Flight Tracker")
st.markdown("Kafka -> Spark -> PostgreSQL 파이프라인 작동 중")

# ★ 빈 공간(Placeholder) 생성 (루프 밖에서 한 번만!)
placeholder = st.empty()

while True:
    # 데이터 가져오기
    total_count, df = load_data()
    
    # ★ 이 안에서 그리는 모든 것은 1초마다 지워지고 다시 그려집니다.
    with placeholder.container():
        if not df.empty:
            # [중요] 컬럼(기둥)을 여기서 만들어야 매번 새로 그려집니다.
            kpi1, kpi2, kpi3 = st.columns(3)

            kpi1.metric(
                label="누적 수집 데이터 수",
                value=f"{total_count} 건"
            )
            kpi2.metric(
                label="최신 항공기 평균 고도 (최근 100개 기준)",
                value=f"{int(df['baro_altitude'].mean())} m"
            )
            kpi3.metric(
                label="최신 항공기 평균 속도 (최근 100개 기준)",
                value=f"{int(df['velocity'].mean())} km/h"
            )

            # 탭 구성 및 지도
            tab1, tab2 = st.tabs(["지도 시각화", "데이터프레임"])
            
            with tab1:
                # 지도 그리기
                st.map(df[['latitude', 'longitude']])
            
            with tab2:
                # 표 그리기
                st.dataframe(df)
        else:
            st.warning("데이터가 아직 없습니다.")
            
    time.sleep(1)