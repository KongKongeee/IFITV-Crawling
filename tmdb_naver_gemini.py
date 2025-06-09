# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 12:08:16 2025

@author: Admin
"""

import os
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import html
from urllib.parse import quote
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import google.generativeai as genai
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
    
# 디렉토리 생성 
os.makedirs('./data_crawling_tmdb_gemini', exist_ok=True)

base_dir = os.path.dirname(__file__)
file_path = os.path.join(base_dir, 'desc_keywords.json')

with open(file_path, 'r', encoding='utf-8') as f:
    desc_keywords = json.load(f)

USE_TMDB_DESC_PRIORITY = True
USE_TMDB_SUBGENRE_PRIORITY = True

tmdb_genre_map = {
    28: '액션',
    12: '모험',
    16: '애니메이션',
    35: '코미디',
    80: '스릴러',
    99: '다큐멘터리',
    18: '드라마',
    14: '판타지',
    27: '공포',
    9648: '미스터리',
    10749: '로맨스',
    878: 'SF',
    10770: '드라마',  # TV 영화 → 드라마 처리
    53: '스릴러',
    10752: '액션',  # 전쟁 → 액션 대체
    37: '모험'  # 서부극 → 모험으로 통합
}

allowed_subgenres_by_genre = {
    '드라마': [
        '해외드라마', '미국드라마', '영국드라마', '중국드라마', '일본드라마',
        '로맨스', '코미디', '판타지', '무협', '공포', '복수', '휴먼', '범죄 스릴러_수사극',
        '의학', '웹툰_소설 원작', '정치_권력', '법정', '청춘', '오피스 드라마', '사극_시대극', '타임슬립'
    ],
    '예능': [
        '버라이어티', '다큐멘터리', '여행', '쿡방/먹방', '연애리얼리티', '게임', '토크쇼', '서바이벌',
        '관찰리얼리티', '스포츠예능', '교육예능', '힐링예능', '아이돌', '음악서바이벌', '음악예능',
        '코미디', '가족예능', '뷰티', '애니멀', '교양'
    ],
    '영화': [
        '드라마', '로맨스', '코미디', '애니메이션', '스릴러', '미스터리',
        '모험', '액션', '판타지', 'SF', '공포', '다큐멘터리'
    ],
    '애니': ['키즈'],
    '보도': ['보도']
}

genre_name_to_kor = {
                "Action": "액션",
                "Thriller": "스릴러",
                "Comedy": "코미디",
                "Drama": "드라마",
                "Romance": "로맨스",
                "Fantasy": "판타지",
                "Science Fiction": "SF",
                "Mystery": "미스터리",
                "Animation": "애니메이션",
                "Horror": "공포",
                "Documentary": "다큐멘터리",
                "Adventure": "모험",
                "Talk": "토크쇼",
                "Reality": "버라이어티",
                "Sci-Fi & Fantasy": "판타지",
            }

def clean_subgenre_by_genre(original_genre, sub_genre):
    if sub_genre == '코미디':
        return '코미디'
    # 예능에 들어가면 안 되는 드라마용 서브장르
    if original_genre == '예능' and sub_genre in [
        '휴먼', '로맨스', '판타지', '무협', '공포', '복수', '의학',
        '웹툰_소설 원작', '정치_권력', '법정', '청춘', '오피스 드라마',
        '사극_시대극', '타임슬립', '범죄 스릴러_수사극'
    ]:
        return ''
    # 드라마에 예능용 서브장르가 잘못 들어온 경우 제거 (예: 뷰티)
    if original_genre == '드라마' and sub_genre not in allowed_subgenres_by_genre['드라마']:
        return ''
    return sub_genre



# 장르 변환 맵
genre_map = {'연예/오락': '예능', '뉴스/정보': '보도', '만화': '애니'}

def guess_subgenre_by_desc(desc):
    # 전처리: 소문자 + 특수문자 제거 + 공백 정리
    desc_clean = re.sub(r'[^\w\s]', ' ', desc).lower()
    desc_clean = re.sub(r'\s+', ' ', desc_clean).strip()

    for subgenre, keywords in desc_keywords.items():
        for keyword in keywords:
            keyword_clean = keyword.lower().strip()
            if keyword_clean in desc_clean:
                return subgenre
    return ''

def clean_title_for_tmdb(title):
    # 괄호 및 특수문자 제거
    title = re.sub(r'[\(\)\[\]〈〉“”"\'\:\-\|·,~!@#\$%\^&\*\+=]+', ' ', title)
    title = re.sub(r'\s+', ' ', title)
    return title.strip()
    

def get_program_info_from_tmdb(title, original_genre):
    
    api_key = os.getenv("TMDB_API_KEY")
    image_base_url = "https://image.tmdb.org/t/p/w500"
    
    # 예외 처리
    if original_genre in ["드라마", "예능", "보도"]:
        endpoints = [("tv", "name"), ("movie", "title")]
    else:
        endpoints = [("movie", "title"), ("tv", "name")] 
        
    cleaned_title = clean_title_for_tmdb(title)

    for content_type, title_key in endpoints:
        try:
            search_url = f"https://api.themoviedb.org/3/search/{content_type}"
            params = {"api_key": api_key, "query": title, "language": "ko-KR"}
            search_res = requests.get(search_url, params=params)
            search_res.raise_for_status()
            results = search_res.json().get("results", [])
            

            if not results:
                continue

            item = results[1] if title == '인간극장' and len(results) > 1 else results[0]
            content_id = item["id"]

            detail_url = f"https://api.themoviedb.org/3/{content_type}/{content_id}"
            detail_res = requests.get(detail_url, params={"api_key": api_key, "language": "ko-KR"})
            detail_res.raise_for_status()
            detail = detail_res.json()

            desc = detail.get("overview", "")
            poster_path = detail.get("poster_path")
            thumbnail = image_base_url + poster_path if poster_path else ''

            genre_data = detail.get("genres", [])
            genre_ids = [g.get("id") for g in genre_data if g.get("id") is not None]
            subgenres = list({tmdb_genre_map.get(gid) for gid in genre_ids if tmdb_genre_map.get(gid)})

            credits_url = f"https://api.themoviedb.org/3/{content_type}/{content_id}/credits"
            credits_res = requests.get(credits_url, params={"api_key": api_key})
            credits = credits_res.json()
            cast_list = [c["name"] for c in credits.get("cast", [])[:5]]
            cast = ', '.join(cast_list)
            age_rating = ''
            try:
                if content_type == "tv":
                    rating_url = f"https://api.themoviedb.org/3/tv/{content_id}/content_ratings"
                else:
                    rating_url = f"https://api.themoviedb.org/3/movie/{content_id}/release_dates"
                
                rating_res = requests.get(rating_url, params={"api_key": api_key})
                rating_res.raise_for_status()
                rating_json = rating_res.json()
            
                if content_type == "tv":
                    for entry in rating_json.get("results", []):
                        if entry.get("iso_3166_1") == "KR":
                            age_rating = entry.get("rating", "")
                            break
                else:
                    for entry in rating_json.get("results", []):
                        if entry.get("iso_3166_1") == "KR":
                            for release in entry.get("release_dates", []):
                                if release.get("certification"):
                                    age_rating = release["certification"]
                                    break
            except:
                age_rating = ''

            if not subgenres:
                fallback_names = [genre_name_to_kor.get(g.get("name"), '') for g in genre_data]
                subgenres = [name for name in fallback_names if name]

            sub_genre = ', '.join(subgenres).strip()

            return desc, thumbnail, sub_genre, age_rating, cast

        except Exception as e:
            print(f"[TMDb 오류 - {content_type.upper()}] {title}: {e}")
            continue

    return '', '', '', '', ''

def validate_and_fix_subgenre(original_genre, sub_genre, desc, genre_text):
    allowed_list = allowed_subgenres_by_genre.get(original_genre, [])
    if not sub_genre or not any(
        sg.strip() in allowed_list for sg in sub_genre.split(',')
    ):
        guessed = guess_subgenre_by_desc((genre_text or '') + " " + (desc or ''))
        guessed = clean_subgenre_by_genre(original_genre, guessed)
        if guessed in allowed_list:
            return guessed
        else:
            return ''
    return sub_genre

def clean_program_name_for_url(name):
    name = re.sub(r'\<.*?\>', '', name)
    name = re.sub(r'\[.*?\]', '', name)
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'〈.*?〉', '', name)
    name = re.sub(r'[“”"\':\-]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def clean_text(text):
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'〈.*?〉', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r',\s*,', ',', text)
    return text

def clean_name(text):
    # ① 괄호 및 특수 괄호 안의 내용 제거
    text = re.sub(r'\([^)]*\)', '', text)      # (내용)
    text = re.sub(r'\[[^\]]*\]', '', text)     # [내용]
    text = re.sub(r'〈.*?〉', '', text)         # 〈내용〉
    text = re.sub(r'\<.*?\>', '', text)        # <내용>
    
    # ② 방송 상태 관련 단어 제거
    text = re.sub(r'\b(수목드라마|월화드라마|일일드라마|재방송\
                  |특별판|스페셜|본방송|본|재|특집|종영|마지막회\
                  |최종화|HD|SD|NEW|다시보기)\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\d+부', '', text)  # 회차 정보 제거
    
    # ③ 특수문자 정리
    text = re.sub(r'[“”"\'\:\-\|·,~!@#\$%\^&\*\+=]+', ' ', text)  # 기호 → 공백
    text = re.sub(r'\s+', ' ', text)  # 연속 공백 정리
    
    # ④ 한글/영문 조합 불필요한 공백 제거
    text = re.sub(r'([가-힣])\s+([A-Za-z])', r'\1\2', text)
    text = re.sub(r'([A-Za-z])\s+([가-힣])', r'\1\2', text)
    
    # ⑤ 끝에 남은 괄호 등 제거
    text = text.strip("()[]〈〉 ")
    
    # ⑥ 전체 정리 후 반환
    return text.strip()


def get_cast_list_from_naver(program_title):
    try:
        query = f"{program_title} 출연진"
        url = f"https://search.naver.com/search.naver?query={quote(query)}"
        driver.get(url)
        time.sleep(1.5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 1차 시도: 우선 선택자
        primary_selector = (
            '#main_pack > div.sc_new._kgs_broadcast.cs_common_module._broadcast_button_scroller.case_normal.color_13 '
            '> div.cm_content_wrap._broadcast_normal_total > div > div.list_image_info._content > ul > li > div > div > span > a'
        )
        cast_tags = soup.select(primary_selector)
        cast_list = [tag.get_text(strip=True) for tag in cast_tags[:5]]

        # 2차 시도: 백업 선택자
        if not cast_list:
            backup_selector = '#main_pack div.cm_content_wrap._broadcast_normal_total ul li div div strong a'
            cast_tags = soup.select(backup_selector)
            cast_list = [tag.get_text(strip=True) for tag in cast_tags[:5]]

        return ', '.join(cast_list) if cast_list else ''
    
    except Exception as e:
        print(f"[네이버 출연진 오류] {program_title}: {e}")
        return ''



# Gemini 호출
def fill_missing_metadata_with_gemini(program_name, original_genre, desc, sub_genre, thumbnail, age_rating, cast):
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel(model_name="gemini-2.0-flash-lite")

    # ✅ 장르 안전 처리
    genre_safe = original_genre if original_genre else "비어 있음"
    genre_list = ['영화', '드라마', '예능', '애니']

    prompt = f"""
다음은 IPTV 프로그램의 메타데이터입니다. 비어 있는 항목(desc, genre, sub_genre, thumbnail 등)이 있다면 추론하여 채워주세요.

프로그램명: {program_name}
장르(genre): {genre_safe}
설명(desc): {desc or '비어 있음'}
서브장르(sub_genre): {sub_genre or '비어 있음'}
썸네일(thumbnail): {thumbnail or '비어 있음'}
연령등급(age_rating): {age_rating or '비어 있음'}
출연진(cast): {cast or '비어 있음'}

가능한 서브장르 목록:
{', '.join(allowed_subgenres_by_genre.get(original_genre, []))}

❗️주의사항:
- '장르'가 비어 있는 경우에는 반드시 다음 중 하나로만 추론해 주세요: **{', '.join(genre_list)}**
- '서브장르'는 반드시 **해당 장르에 속하는 사전 정의된 목록 중에서만** 추론해 주세요.
- '썸네일'은 반드시 실제 이미지 URL만 작성해 주세요 (예: https://...).
- AI가 상상한 이미지나 일반 묘사일 경우 '정보 없음'으로 작성하세요.
- '연령등급'은 반드시 '전체 이용가', '12세 이상', '15세 이상', '19세 이상' 중 하나로 작성하세요.
- 출연진에 영어 이름이 있다면 반드시 한글로 번역해 주세요 (예: Tom Cruise → 톰 크루즈).

🧾 아래 형식으로만 출력해 주세요 (형식 엄수):
장르: ...
설명: ...
서브장르: ...
썸네일: ...
연령등급: ...
출연진: ...
"""

    try:
        response = model.generate_content(prompt)
        content = response.text.strip()

        # 초기값
        genre_out = original_genre or "정보 없음"
        desc_out = desc or "정보 없음"
        sub_out = sub_genre or "정보 없음"
        thumb_out = thumbnail or "정보 없음"
        age_out = age_rating or "정보 없음"
        cast_out = cast or "정보 없음"

        lines = [line.strip() for line in content.splitlines() if line.strip()]
        for line in lines:
            if line.startswith("장르:"):
                value = line.replace("장르:", "").strip()
                if value: genre_out = value
            elif line.startswith("설명:"):
                value = line.replace("설명:", "").strip()
                if value: desc_out = value
            elif line.startswith("서브장르:"):
                value = line.replace("서브장르:", "").strip()
                if value: sub_out = value
            elif line.startswith("썸네일:"):
                value = line.replace("썸네일:", "").strip()
                if value: thumb_out = value
            elif line.startswith("연령등급:"):
                value = line.replace("연령등급:", "").strip()
                if value: age_out = value
            elif line.startswith("출연진:"):
                value = line.replace("출연진:", "").strip()
                if value: cast_out = value

        return genre_out, sub_out, desc_out, thumb_out, age_out, cast_out

    except Exception as e:
        print(f"[Gemini 오류] {program_name}: {e}")
        return original_genre or "정보 없음", sub_genre or "정보 없음", desc or "정보 없음", thumbnail or "정보 없음", age_rating or "정보 없음", cast or "정보 없음"

def translate_cast_to_korean(cast_english):
    if not cast_english:
        return ''

    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel(model_name="gemini-2.0-flash-lite")

    # cast 이름 목록으로 분리
    cast_list = [name.strip() for name in cast_english.split(',') if name.strip()]
    cast_bullet = '\n'.join(f"- {name}" for name in cast_list)

    prompt = f"""
다음 영어 이름들을 한국어 이름으로 자연스럽게 번역해서 쉼표로 구분된 한 줄로 출력해줘.
- 반드시 원본과 순서를 맞춰서 번역하고, 번역 불가하면 생략하지 말고 그대로 출력해.
- 줄바꿈 없이, '홍길동, 김철수' 형식으로만 출력해.
- 말투나 설명 없이 번역 결과만 출력해.

영어 이름 목록:
{cast_bullet}
    """

    try:
        response = model.generate_content(prompt)
        translated = response.text.strip()

        # 후처리: 줄바꿈 제거 및 쉼표 기준 정리
        translated = re.sub(r'\s+', ' ', translated)
        translated = translated.replace(' ,', ',').replace(', ', ',').replace(',', ', ')
        return translated.strip()

    except Exception as e:
        print(f"[Gemini 번역 오류 - cast] {cast_english}: {e}")
        return cast_english


def get_info_from_web_search(name):
    cleaned = clean_name(name)
    query = f"{cleaned} 정보"
    driver.get(f"https://search.naver.com/search.naver?query={quote(query)}")
    time.sleep(1.5)

    try:
        genre = driver.find_element(By.CSS_SELECTOR, "div.sub_title span").text.strip()
    except:
        genre = ''

    try:
        thumbnail = driver.find_element(
            By.CSS_SELECTOR,
            '#main_pack div[class*="_broadcast_button_scroller"] div.cm_content_wrap._broadcast_normal_total > div:nth-child(1) div.detail_info a img'
        ).get_attribute("src")
    except:
        thumbnail = ''

    return genre, thumbnail

# 메타데이터 호출
def get_program_metadata(program_name, driver, original_genre):
    name = clean_name(program_name)

    # ✅ 예외처리 테이블
    program_exceptions = {
        '세계테마기행': {
            'genre': '예능',
            'desc': '단순한 여행 정보 프로그램에서 벗어나, 자유로운 배낭여행자만이 느낄 수 있는 살아있는 체험기를 전하는 다큐멘터리 프로그램',
            'sub_genre': '여행, 다큐멘터리',
            'thumbnail': 'https://image.tmdb.org/t/p/w500/pHC70ke34d0pEOdhcx8lnWhRtqk.jpg',
            'age_rating': '전체 이용가',
            'cast': '정보 없음'
        },
        
    }

    # ✅ 프로그램명 기반 예외처리
    if name in program_exceptions:
        meta = program_exceptions[name]
        genre = meta.get('genre', original_genre)  # 없으면 기존 값 유지
        return genre, meta['sub_genre'], meta['desc'], meta['thumbnail'], meta['age_rating'], meta['cast']

    # ✅ 장르 기반 예외처리 (만화 포함)
    if original_genre in ['스포츠', '애니', '만화']:
        genre_map = {
            '스포츠': ('스포츠', program_name),
            '애니': ('키즈', program_name),
            '만화': ('키즈', program_name),
        }
        sub_genre, desc = genre_map[original_genre]
        return '애니' if original_genre == '만화' else original_genre, sub_genre, desc, '', '전체 이용가', '정보 없음'
    
    # ✅ TMDb 단일 소스
    desc, thumbnail, sub_genre, age_rating, cast = get_program_info_from_tmdb(name, original_genre)

    # ✅ NAVER 검색 (보조 정보 추출용)
    genre_text, web_thumb = get_info_from_web_search(name)
    if not thumbnail:
        thumbnail = web_thumb

    # ✅ 영어 cast → 한글 번역
    if cast and all(ord(c) < 128 for c in cast):
        cast = translate_cast_to_korean(cast)

    # ✅ cast 없으면 Naver로 보완
    if not cast or cast == "정보 없음":
        cast_from_naver = get_cast_list_from_naver(program_name)
        if cast_from_naver:
            cast = cast_from_naver

    # ✅ 장르 정합성 보정
    if genre_text == '시사/교양':
        original_genre = '예능'
        sub_genre = '교양'
        
    if genre_text == '시사/보도':
        original_genre = '보도'
    
    if genre_text == '애니':
        sub_genre = '키즈'

    if sub_genre and isinstance(sub_genre, str):
        keywords = ['교육', '어린이', 'TV만화', '키즈', '유아교육', '유아 교육', '유아/어린이']
        if any(sg.strip() in keywords for sg in sub_genre.split(',')):
            original_genre, sub_genre = '애니', '키즈'

    if sub_genre and isinstance(sub_genre, str):
        keywords = ['영어 회화', '교육', '과학', '초급 영어', '초등', '중등', '고등']
        if any(sg.strip() in keywords for sg in sub_genre.split(',')):
            original_genre, sub_genre = '예능', '교육예능'
        
    if original_genre in ['스포츠', '보도']:
        sub_genre = original_genre
        desc = program_name
    if original_genre == '공연/음악':
        original_genre, sub_genre = '예능', '음악예능'
    if original_genre == '영화':
        forbidden = set(allowed_subgenres_by_genre['예능'] + ['범죄 스릴러_수사극'])
        if sub_genre in forbidden:
            sub_genre = ''

    # ✅ 정합성 필터링 및 자동 추정
    sub_genre = clean_subgenre_by_genre(original_genre, sub_genre)
    sub_genre = validate_and_fix_subgenre(original_genre, sub_genre, desc, genre_text)



    # ✅ Gemini 보완
    if not original_genre or not desc or not sub_genre or not thumbnail or not age_rating or not cast:
        genre_out, sub_genre, desc, thumbnail, age_rating, cast = fill_missing_metadata_with_gemini(
            program_name, original_genre, desc, sub_genre, thumbnail, age_rating, cast
        )
        original_genre = genre_out

    desc = re.sub(r'\s+', ' ', desc or '').strip()
    return original_genre, sub_genre, desc, thumbnail, age_rating, cast




        # 런타임 계산
def calculate_runtime(programs):
    new_list = []
    for i in range(len(programs)):
        current_time = datetime.strptime(programs[i][1], "%H:%M:%S")  # time_text는 인덱스 1
        if i < len(programs) - 1:
            next_time = datetime.strptime(programs[i + 1][1], "%H:%M:%S")
            if next_time < current_time:
                next_time += timedelta(days=1)
            runtime = int((next_time - current_time).total_seconds() / 60)
        else:
            runtime = 60
        new_list.append(programs[i] + [runtime])  # 기존 리스트 + runtime 붙이기
    return new_list



# 채널 리스트
channel_list = [
    # 전국 지상파
    'KBS1[9]', 'KBS2[7]', 'MBC[11]', 'SBS[5]',

    # 종편 + 공영 + 교양
    'JTBC[15]', 'MBN[16]', '채널A[18]', 'TV조선[19]',
    'EBS1[14]', 'EBS2[95]', 'OBS[26]',

    # 드라마/예능/영화 전문 채널
    'tvN[3]', 'OCN[44]', '스크린[46]', '씨네프[47]', 'OCN Movies2[51]',
    '캐치온1[52]', '캐치온2[53]', '채널액션[54]',
    '드라마큐브[71]', 'ENA[72]', 'ENA DRAMA[73]',
    'KBS Story[74]', 'SBS플러스[33]', 'MBC드라마넷[35]', # 필요시 추가

    # 애니메이션/키즈 채널
    '투니버스[324]', '카툰네트워크[316]',
    '애니박스[327]', '애니맥스[326]', '어린이TV[322]' # 필요시 추가
]

# 크롬 드라이버 설정
options = Options()
# options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(options=options)
start_time = time.time()
print("[크롤링 시작]")
wait = WebDriverWait(driver, 10)

url = 'https://www.lguplus.com/iptv/channel-guide'
'''
driver.get(url)
driver.execute_script("document.body.style.zoom='50%'")
time.sleep(2)
'''
table_btn_xpath = '//a[contains(text(), "채널 편성표 안내")]'
all_channel_btn_xpath = '//a[contains(text(), "전체채널")]'


# 채널별 반복 크롤링
for channel in channel_list:
    try:
        driver.get(url)
        driver.execute_script("document.body.style.zoom='50%'")
        time.sleep(1)

        wait.until(EC.element_to_be_clickable((By.XPATH, table_btn_xpath))).click()
        time.sleep(1)
        wait.until(EC.element_to_be_clickable((By.XPATH, all_channel_btn_xpath))).click()
        time.sleep(2)

        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.c-btn-outline-2-s.open"))).click()
        time.sleep(1)

        channel_xpath = f'//a[contains(text(), "{channel}")]'
        wait.until(EC.element_to_be_clickable((By.XPATH, channel_xpath))).click()
        time.sleep(2)

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        program_soup_list = soup.select('tr.point')

        temp_list = []
        for item in program_soup_list:
            try:
                tds = item.select('td')
                time_text = tds[0].text.strip()
                name_parts = tds[1].text.split('\n')
                raw_name = name_parts[1].strip() if len(name_parts) > 1 else tds[1].text.strip()
                name = clean_name(raw_name)
                if name in ["방송 시간이 아닙니다", "방송시간이 아닙니다.", "방송시간이 아닙니다"]:
                    continue
                genre = genre_map.get(tds[2].text.strip(), tds[2].text.strip())
                temp_list.append([channel, time_text, name, genre])
            except Exception as e:
                print(f"[파싱 오류] {e}")
                continue

        temp_list = calculate_runtime(temp_list)

        # 중복 병합 (runtime 합산)
        merged_programs = [] 
        skip_next = False
        for i in range(len(temp_list)):
            if skip_next:
                skip_next = False
                continue
            if i < len(temp_list) - 1 and temp_list[i][1] == temp_list[i + 1][1]:
                merged = temp_list[i][:]
                merged[3] = temp_list[i][3] + temp_list[i + 1][3]
                merged_programs.append(merged)
                skip_next = True
            else:
                merged_programs.append(temp_list[i])



        def fetch_metadata(channel, airtime, title, genre, runtime, driver, metadata_cache):
            try:
                # ✅ 캐시가 있으면 그대로 사용
                if title in metadata_cache:
                    genre_out, sub_genre, desc, thumbnail, age_rating, cast = metadata_cache[title]
                else:
                    # ✅ 메타데이터 추출
                    genre_out, sub_genre, desc, thumbnail, age_rating, cast = get_program_metadata(title, driver, genre)
                    metadata_cache[title] = (genre_out, sub_genre, desc, thumbnail, age_rating, cast)
        
                return [channel, airtime, title, genre_out, sub_genre, runtime, desc, thumbnail, age_rating, cast]
        
            except Exception as e:
                print(f"[메타데이터 오류] {title}: {e}")
                return None


        final_list = []
        metadata_cache = {}
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(fetch_metadata, channel, airtime, title, genre, runtime, driver, metadata_cache): title
                for channel, airtime, title, genre, runtime in merged_programs
            }
        
            for future in as_completed(futures):
                result = future.result()
                if result:
                    final_list.append(result)
                time.sleep(0.1)  # ✅ Gemini 과다 호출 방지

        safe_name = re.sub(r'\s*(\[[^]]*\])', '', channel).strip()
        df = pd.DataFrame(final_list, columns = ['channel','airtime', 'title', 'genre', 'subgenre','runtime', 'description', 'thumbnail', 'age_rating', 'cast'])
        df['subgenre'] = df['subgenre'].apply(lambda x: x.replace('"', '') if isinstance(x, str) else x)
        df = df.sort_values(by='airtime')
        df.to_csv(f'./data_crawling_tmdb_gemini/{safe_name}_program_list.csv', index=False, encoding='utf-8-sig')

        print(f"[완료] {channel} → 저장 완료")
        time.sleep(1)

    except Exception as e:
        print(f"[채널 오류] {channel} 처리 중 오류:\n{traceback.format_exc()}")
        continue

driver.quit()
end_time = time.time()
elapsed = end_time - start_time
minutes = int(elapsed // 60)
seconds = int(elapsed % 60)
print(f"[전체 완료] 모든 채널 크롤링 종료 (총 소요 시간: {minutes}분 {seconds}초)")
