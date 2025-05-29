# -*- coding: utf-8 -*-
"""
Created on Fri May  27 15:10:39 2025

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

def get_program_info_from_tmdb(title, original_genre):
    def clean_title_for_tmdb(title):
    # 괄호 및 특수문자 제거
        title = re.sub(r'[\(\)\[\]〈〉“”"\'\:\-\|·,~!@#\$%\^&\*\+=]+', ' ', title)
        title = re.sub(r'\s+', ' ', title)
        return title.strip()
    
    api_key = os.getenv("TMDB_API_KEY")
    image_base_url = "https://image.tmdb.org/t/p/w500"
    
    # 예외 처리
    if original_genre in ["드라마", "예능"]:
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

            if not subgenres:
                fallback_names = [genre_name_to_kor.get(g.get("name"), '') for g in genre_data]
                subgenres = [name for name in fallback_names if name]

            sub_genre = ', '.join(subgenres).strip()

            return desc, thumbnail, sub_genre, cast

        except Exception as e:
            print(f"[TMDb 오류 - {content_type.upper()}] {title}: {e}")
            continue

    return '', '', '', ''

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




def get_program_info_from_tvmaze(title):
    try:
        search_url = f"https://api.tvmaze.com/search/shows?q={quote(title)}"
        res = requests.get(search_url, timeout=3)
        res.raise_for_status()
        results = res.json()
        if not results:
            return '', '', '', ''

        show = results[0]["show"]
        tvmaze_id = show["id"]
        
        desc = html.unescape(show.get("summary", "").replace('<p>', '').replace('</p>', ''))
        thumbnail = show.get("image", {}).get("original", '') or show.get("image", {}).get("medium", '')
        genres = show.get("genres", [])
        sub_genre = ', '.join(genres)
        cast_url = f"https://api.tvmaze.com/shows/{tvmaze_id}/cast"
        cast_res = requests.get(cast_url, timeout=3)
        cast_json = cast_res.json()
        cast_list = [entry["person"]["name"] for entry in cast_json[:5]]
        cast = ', '.join(cast_list)

        return desc.strip(), thumbnail, sub_genre, cast

    except Exception as e:
        print(f"[TVmaze 오류] {title}: {e}")
        return '', '', '', ''

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

def extract_subgenre(soup):
    a_tags = soup.select('table.infobox td a')
    for a in a_tags:
        txt = a.get_text(strip=True)
        for sub in desc_keywords.keys():
            if any(kw in txt for kw in desc_keywords[sub]):
                return sub
    return ''


def get_info_from_korean_wikipedia(program_name):

    # 클린 처리
    clean_name = clean_program_name_for_url(program_name)
    name_parts = clean_name.split()
    headers = {"User-Agent": "Mozilla/5.0"}

    # 점진적 축소 검색
    for i in range(len(name_parts), 0, -1):
        try_name = ' '.join(name_parts[:i])
        wiki_url = f"https://ko.wikipedia.org/wiki/{quote(try_name)}"

        try:
            res = requests.get(wiki_url, headers=headers, timeout=3)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')

            # 설명 추출
            desc = ''
            for p in soup.select('div.mw-parser-output > p'):
                text = p.get_text(strip=True)
                if len(text) > 50:
                    desc = clean_text(text)
                    break

            # 서브장르 추출
            sub_genre = extract_subgenre(soup)

            return desc, sub_genre  # 성공한 경우 즉시 반환

        except Exception:
            continue  # 실패한 경우 다음 이름 시도

    return '', ''  # 전부 실패 시 빈 값 반환


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




def fill_missing_metadata_with_gemini(program_name, original_genre, desc, sub_genre, thumbnail, cast):
    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
    model = genai.GenerativeModel(model_name="gemini-2.0-flash-lite")

    prompt = f"""
    다음은 IPTV 프로그램의 메타데이터입니다. 비어 있는 항목(desc, sub_genre, thumbnail)이 있다면 추론하여 채워주세요.

    프로그램명: {program_name}
    장르: {original_genre}
    설명(desc): {desc or '비어 있음'}
    서브장르(sub_genre): {sub_genre or '비어 있음'}
    썸네일(thumbnail): {thumbnail or '비어 있음'}
    출연진(cast): {cast or '비어 있음'}

    가능한 서브장르 목록:
    {', '.join(allowed_subgenres_by_genre.get(original_genre, []))}

    ❗️주의사항:
        - '썸네일'은 반드시 **실제 이미지 URL**일 경우에만 작성해주세요 (예: https://... 로 시작하는 주소).
        - **추상적 이미지, 상징적 그림** 등의 일반 묘사일 경우, **무조건 "정보 없음"으로 기재**해주세요.
        - 의미 없는 꾸밈이나 AI가 상상한 장면은 포함하지 마세요.
        - '서브장르'는 반드시 **"가능한 서브장르 목록"에서만** 추론해주세요.
        - '서브장르'를 알 수 없을 시 **무조건 "정보 없음"으로 기재해주세요.
        - 출연진에 영어 이름이 포함되어 있다면 반드시 한글 이름으로 번역해주세요. 예: "Tom Cruise" → "톰 크루즈"


    아래 형식으로만 출력해주세요 (형식 엄수):
    설명: ...
    서브장르: ...
    썸네일: ...
    출연진: ...
    """

    try:
        response = model.generate_content(prompt)
        content = response.text.strip()

        # 기본값 지정
        desc_out = desc or "정보 없음"
        sub_out = sub_genre or "정보 없음"
        thumb_out = thumbnail or "정보 없음"
        cast_out = cast or "정보 없음"

        # 형식 검증: 정확히 3줄, 키워드가 반드시 포함되어야 함
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        found = {"설명": False, "서브장르": False, "썸네일": False}

        for line in lines:
            if line.startswith("설명:"):
                value = line.replace("설명:", "").strip()
                desc_out = value if value else "정보 없음"
                found["설명"] = True
            elif line.startswith("서브장르:"):
                value = line.replace("서브장르:", "").strip()
                sub_out = value if value else "정보 없음"
                found["서브장르"] = True
            elif line.startswith("썸네일:"):
                value = line.replace("썸네일:", "").strip()
                thumb_out = value if value else "정보 없음"
                found["썸네일"] = True
            elif line.startswith("출연진:"):
                cast_out = line.replace("출연진:", "").strip()

        # 누락된 항목 보완
        if not found["설명"] and not desc:
            desc_out = "정보 없음"
        if not found["서브장르"] and not sub_genre:
            sub_out = "정보 없음"
        if not found["썸네일"] and not thumbnail:
            thumb_out = "정보 없음"

        return desc_out, sub_out, thumb_out, cast_out

    except Exception as e:
        print(f"[Gemini 오류] {program_name}: {e}")
        return desc or "정보 없음", sub_genre or "정보 없음", thumbnail or "정보 없음", cast or "정보 없음"




def get_program_metadata(program_name, driver, original_genre):
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

    name = clean_name(program_name)

    # TMDb
    tmdb_desc, tmdb_thumb, tmdb_sub, tmdb_cast = get_program_info_from_tmdb(name, original_genre)

    # TVmaze (fallback)
    if not tmdb_desc and not tmdb_sub and not tmdb_cast:
        tvmaze_desc, tvmaze_thumb, tvmaze_sub, tvmaze_cast = get_program_info_from_tvmaze(name)
    else:
        tvmaze_desc = tvmaze_thumb = tvmaze_sub = tvmaze_cast = ''

    # Wikipedia
    wiki_desc, wiki_sub = get_info_from_korean_wikipedia(name)

    # NAVER
    web_genre, web_thumb = get_info_from_web_search(name)

    # 설명 우선순위
    desc = tmdb_desc if USE_TMDB_DESC_PRIORITY and tmdb_desc else max([tvmaze_desc, wiki_desc], key=lambda x: len(x or ''))

    # 썸네일 우선순위
    thumbnail = tmdb_thumb or tvmaze_thumb or web_thumb or ''

    # 서브장르 우선순위
    sub_genre = tmdb_sub if USE_TMDB_SUBGENRE_PRIORITY and tmdb_sub else tvmaze_sub or wiki_sub

    # 출연진 우선순위
    cast = tmdb_cast or tvmaze_cast
    
    if not cast or all(ord(c) < 128 for c in cast):  # 모두 영문이면
        cast_from_naver = get_cast_list_from_naver(program_name)
        if cast_from_naver:
            cast = cast_from_naver

    # 장르 정합성 보정
    if web_genre == '시사/교양':
        original_genre = '예능'
        sub_genre = '교양'
    if sub_genre in ['어린이', 'TV만화', '키즈']:
        original_genre, sub_genre = '애니', '키즈'
    if original_genre == '스포츠':
        original_genre, sub_genre = '예능', '스포츠예능'
    if original_genre == '보도':
        sub_genre = '보도'
    if original_genre == '공연/음악':
        original_genre, sub_genre = '예능', '음악예능'

    # 영화 장르일 때 예능 서브장르 제거
    if original_genre == '영화':
        forbidden = set(allowed_subgenres_by_genre['예능'] + ['범죄 스릴러_수사극'])
        if sub_genre in forbidden:
            sub_genre = ''

    # 정합성 필터링 및 보완
    sub_genre = clean_subgenre_by_genre(original_genre, sub_genre)
    sub_genre = validate_and_fix_subgenre(original_genre, sub_genre, desc, web_genre)

    # Gemini 보완 (desc, sub_genre, thumbnail, cast)
    if not desc or not sub_genre or not thumbnail or not cast:
        desc, sub_genre, thumbnail, cast = fill_missing_metadata_with_gemini(
            program_name, original_genre, desc, sub_genre, thumbnail, cast
        )

    # 키즈 처리 보완
    if sub_genre in ['유아교육', '유아 교육', '유아/어린이']:
        original_genre, sub_genre = '애니', '키즈'
    if sub_genre in ['영어 회화']:
        original_genre, sub_genre = '예능', '교육예능'

    desc = re.sub(r'\s+', ' ', desc).strip()
    return original_genre, sub_genre, desc, thumbnail, cast




        # 런타임 계산
def calculate_runtime(programs):
    new_list = []
    for i in range(len(programs)):
        current_time = datetime.strptime(programs[i][0], "%H:%M:%S")
        if i < len(programs) - 1:
            next_time = datetime.strptime(programs[i + 1][0], "%H:%M:%S")
            if next_time < current_time:
                next_time += timedelta(days=1)
            runtime = int((next_time - current_time).total_seconds() / 60)
        else:
            runtime = 60
        new_list.append(programs[i][:4] + [runtime] + programs[i][4:])
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
                if name in ["방송 시간이 아닙니다", "방송시간이 아닙니다."]:
                    continue
                genre = genre_map.get(tds[2].text.strip(), tds[2].text.strip())
                temp_list.append([time_text, name, genre])
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

        

        final_list = []
        for airtime, title, genre, runtime in merged_programs:
            try:
                original_genre, sub_genre, desc, thumbnail, cast = get_program_metadata(title, driver, genre)
                final_list.append([channel,airtime, title, original_genre, sub_genre, runtime, desc, thumbnail, cast])
                time.sleep(0.2)
            except Exception as e:
                print(f"[메타데이터 오류] {title}: {e}")
                continue

        safe_name = re.sub(r'\s*(\[[^]]*\])', '', channel).strip()
        df = pd.DataFrame(final_list, columns = ['channel','airtime', 'title', 'genre', 'subgenre','runtime', 'description', 'thumbnail', 'cast'])
        df['subgenre'] = df['subgenre'].apply(lambda x: x.replace('"', '') if isinstance(x, str) else x)
        df.to_csv(f'./data_crawling_tmdb_gemini/{safe_name}_program_list.csv', index=False, encoding='utf-8-sig')

        print(f"[완료] {channel} → 저장 완료")
        time.sleep(1)

    except Exception as e:
        print(f"[채널 오류] {channel} 처리 중 오류:\n{traceback.format_exc()}")
        continue

driver.quit()
print("[전체 완료] 모든 채널 크롤링 종료")