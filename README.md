# LG U+ IPTV 채널 편성표 크롤러

이 프로젝트는 **LG U+ IPTV 채널의 편성표를 자동으로 크롤링**하고, 각 프로그램의 메타데이터(설명, 장르, 서브장르, 썸네일, 출연진 등)를 **TMDb, TVmaze, Wikipedia, NAVER, Gemini API**를 통해 보완한 후 CSV 파일로 저장하는 자동화 도구입니다.

---

## 📦 기능 요약

- LG U+ 공식 웹사이트에서 **채널별 프로그램 편성표 수집**
- 프로그램 설명 / 썸네일 / 출연진 / 장르 및 서브장르 자동 보완:
  - TMDb API
  - TVmaze API
  - Wikipedia (한국어)
  - NAVER 검색 (BeautifulSoup + Selenium)
  - Google Gemini API (보완용)
- 프로그램 런타임 자동 계산
- 이상값(불일치 서브장르, 무효 설명 등) 자동 보정
- 채널별 CSV 저장: `./data_crawling_tmdb_gemini/{채널명}_program_list.csv`

---

## 🛠️ 설치 및 환경 구성

### 1. Python 패키지 설치

pip install -r requirements.txt

> requirements.txt 예시:
selenium
beautifulsoup4
pandas
requests
python-dotenv
google-generativeai

### 2. ChromeDriver 설치

- https://chromedriver.chromium.org/downloads
- 시스템 PATH에 추가하거나, webdriver.Chrome() 경로 직접 지정

### 3. .env 파일 설정

TMDB_API_KEY=YOUR_TMDB_API_KEY
GOOGLE_API_KEY=YOUR_GEMINI_API_KEY

---

## ▶️ 실행 방법

python your_script_name.py

스크립트 실행 시 모든 채널에 대해 다음 순서로 자동 처리됩니다:
1. 웹 페이지에서 편성표 수집
2. 프로그램명 기반 메타데이터 수집 (API + 크롤링)
3. 정합성 검토 및 보완
4. 채널별로 CSV 저장

---

## 📁 결과물 파일 구조

data_crawling_tmdb_gemini/
├── KBS1_program_list.csv
├── SBS_program_list.csv
├── ...

CSV 파일 컬럼 설명:

| channel | airtime | title | genre | subgenre | runtime | description | thumbnail | cast |
|---------|---------|-------|-------|----------|---------|-------------|-----------|------|

---

## ⚠️ 참고사항

- Gemini API 사용 시 https://makersuite.google.com/app 에서 API 키 발급
- TMDb API는 영화/TV 기반, IPTV 특성상 일부 프로그램은 일치하지 않을 수 있음
- headless 모드는 기본 비활성화 상태, CLI 환경에서는 주석 해제
