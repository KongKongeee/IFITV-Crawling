# IFITV-Crawling 📺

LG U+ IPTV 채널 편성표를 자동 크롤링하고, 프로그램 메타데이터(장르, 서브장르, 설명, 썸네일)를 수집하여 CSV로 저장하는 Python 기반 크롤링 프로젝트입니다.

## 🛠 주요 기능

- LG U+ 사이트에서 각 채널의 실시간 편성표 크롤링
- TMDb, TVmaze, NAVER, Wikipedia 기반 프로그램 정보 수집
- 장르 및 서브장르 자동 추정 및 정제
- 프로그램 이름 클렌징 및 런타임 계산
- `.csv`로 저장
- `.env` 환경변수 활용으로 API 키 보안

---

## 🗂 디렉토리 구조

```
project/
│
├── crawling_live.py             # 메인 크롤링 실행 파일
├── desc_keywords.json           # 서브장르 추정용 키워드 정의
├── requirements.txt             # 설치 패키지 목록
├── .env                         # TMDb API Key 등 환경변수
├── .gitignore                   # 민감 파일 제외
└── data_crawling_live/         # 크롤링된 결과 저장 폴더
```

---

## ⚙️ 설치 방법

```bash
# 가상환경 권장
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\activate (Windows)

# 필수 패키지 설치
pip install -r requirements.txt
```

---

## 🔐 .env 파일 설정

`.env` 파일을 프로젝트 루트에 생성하고 아래와 같이 작성합니다:

```
TMDB_API_KEY=your_tmdb_api_key_here
```

> `.env`는 `.gitignore`에 등록되어 Git에 올라가지 않습니다.

---

## ▶️ 실행 방법

```bash
python crawling_live.py
```

실행 후 `data_crawling_live/` 폴더에 채널별 편성표 CSV 파일이 저장됩니다.

---

## 💡 참고사항

- 프로그램 설명 및 장르 추정은 정확도를 높이기 위해 여러 API와 위키피디아를 병행 사용합니다.
- 크롬 브라우저가 설치되어 있어야 하며, Selenium의 크롬 드라이버 버전이 일치해야 합니다.

---

## 📜 라이선스

MIT License

---

## 👨‍💻 개발자

- GitHub: [@KongKongeee](https://github.com/KongKongeee)
- Project: `IFITV-Crawling`
