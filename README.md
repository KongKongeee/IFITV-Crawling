
# 📺 IPTV 편성표 크롤러

LG U+ 사이트에서 방송 프로그램 정보를 수집하고, TMDb / TVmaze / Wikipedia / NAVER 검색을 통해 **장르 및 서브장르, 설명, 썸네일** 메타데이터를 자동으로 보완하는 **IPTV 편성표 크롤링 시스템**입니다.

## 📂 구성 파일

- `crawling_live.py`  
  → LG U+ IPTV 채널 편성표를 Selenium으로 크롤링하고 프로그램 메타데이터를 추론하여 CSV로 저장합니다.

- `desc_keywords.json`  
  → 설명 키워드를 기반으로 서브장르를 추정하는 키워드 맵 파일입니다.

- `requirements.txt`  
  → 실행 환경에 필요한 라이브러리 버전 명세입니다.

---

## ⚙️ 설치 방법

```bash
git clone https://github.com/yourname/iptv-crawler.git
cd iptv-crawler

# 가상환경 권장
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 필수 라이브러리 설치
pip install -r requirements.txt
```

---

## 🚀 실행 방법

```bash
python crawling_live.py
```

실행 시 `./data_crawling_live/` 디렉토리에 채널별 프로그램 CSV 파일이 생성됩니다.

---

## 🔍 기능 요약

- LG U+ IPTV 웹사이트에서 방송 시간표 및 프로그램명 크롤링
- 프로그램명 기반으로:
  - TMDb → 영화/TV 설명 + 장르/썸네일 수집
  - TVmaze → 보완 정보 수집
  - Wikipedia → 한글 설명 보완
  - NAVER 검색 → 장르/설명/썸네일 추가 수집
- 장르와 서브장르 자동 추론 및 정합성 검증
- 예외 처리:
  - '모험' → '여행' 변환
  - '뷰티' → '휴먼' 보정 등
- `desc_keywords.json` 기반으로 설명 키워드에 따라 서브장르 추정

---

## 🧪 사용된 라이브러리

| 라이브러리        | 버전     |
|------------------|----------|
| pandas           | 2.2.2    |
| requests         | 2.32.3   |
| beautifulsoup4   | 4.12.3   |
| selenium         | 4.28.1   |
