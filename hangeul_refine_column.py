import os
import re
import time
import json

import mariadb

import pandas as pd
import urllib.request
from tqdm import tqdm


if __name__=="__main__":

    # MariaDB Local DB Connection 
    print('==== MariaDB Connection =================================================================')
    conn_mariadb = mariadb.connect(
        user="root",
        password="1234",
        host="127.0.0.1",
        port=3306,
        database="nl2sql"
    )
    cur_mariadb = conn_mariadb.cursor()

    # 
    # 한글 테이블 1차 정재
    sql_word = """ SELECT id, table_id, column_id, word_target, word_hangeul, word_english, word_translated, word_second 
                     FROM word_translate
                    WHERE word_target = 'column'
               """
    cur_mariadb.execute(sql_word)
    result_word = cur_mariadb.fetchall()

    year_pattern1 = re.compile('^[0-9]{4}(년)?$')
    year_pattern2 = re.compile('^[0-9]{4}(_)[0-9]{2}$')
    year_pattern3 = re.compile('^[0-9]{4}(_)[0-9]{2}(_)[0-9]{2}$')
    year_pattern4 = re.compile('^[0-9]{2}(년)?$')

    num_pattern = re.compile('^[\d+]$')

    hour_pattern1 = re.compile('^[0-9]+(시)?(_)?[0-9]+(시)?$')
    hour_pattern2 = re.compile('^[0-9]+(시)(\s)?(이전)$')
    hour_pattern3 = re.compile('^[0-9]+(시)$')

    month_pattern = re.compile('^1월$|^2월$|^3월$|^4월$|^5월$|^6월$|^7월$|^8월$|^9월$|^10월$|^11월$|^12월$')
    
    age_pattern1 = re.compile('^[0-9]+(세)?(_)[0-9]+세$')
    age_pattern2 = re.compile('^[0-9]+세$')
    age_pattern3 = re.compile('^만[0-9]+세(_|\s)?남자$')
    age_pattern4 = re.compile('^만[0-9]+세(_|\s)?여자$')
    age_pattern5 = re.compile('^[0-9]+(에서)(_|\s)?[0-9]+(세)(_|\s)?(인구)$')
    age_pattern6 = re.compile('^[0-9]+(세)?(_|\s)?(이상)$')
    age_pattern7 = re.compile('^[0-9]+대$')
    age_pattern8 = re.compile('^[0-9]+(세)?(_|\s)?(이상)(_|\s)?[0-9]+(세)?(_|\s)?(미만)$')
    age_pattern9 = re.compile('^[0-9]+(세)?(_|\s)?(이상)(_|\s)?[0-9]+(세)?(_|\s)?(이하)(_|\s|\()(중위수)?(_|\s|\))?$')
    age_pattern10 = re.compile('^[0-9]+(세)?(_|\s)?(이상)(_|\s)?[0-9]+(세)?(_|\s)?(이하)(_|\s|\()(평균)?(_|\s|\))?$')
    

    people_pattern1 = re.compile('^[0-9]+(인)?(_)[0-9]+(인)$')
    people_pattern2 = re.compile('^[0-9]+(인)$')
    people_pattern3 = re.compile('^[0-9]+(_)[0-9]+(명)(_|\s)?(사업체)(_|\s)?(종사자수)$')
    
    company_pattern = re.compile('^[0-9]+(_)[0-9]+(명)(_|\s)?(사업체수)$')
    grade_pattern = re.compile('^[0-9]+(등급)$')

    other_pattern1 = re.compile('^[0-9]{4}(_|\s)정원$')
    other_pattern2 = re.compile('^[0-9]{4}(_|\s)현원$')

    other_pattern3 = re.compile('^1차_[0-9]{4}_[0-9]{2}_[0-9]{2}$')
    other_pattern4 = re.compile('^2차_[0-9]{4}_[0-9]{2}_[0-9]{2}$')
    other_pattern5 = re.compile('^3차_[0-9]{4}_[0-9]{2}_[0-9]{2}$')
    other_pattern6 = re.compile('^4차_[0-9]{4}_[0-9]{2}_[0-9]{2}$')

    other_pattern7 = re.compile('^[0-9]+(시)(_|\s)?(강우량)$')
    other_pattern8 = re.compile('^(1st)|(2nd)|(3rd)|(4th)|(5th)|(6th)|(7th)|(8th)$')
    other_pattern9 = re.compile('^[0-9]+(_|\s)?(종)$')
    
    other_pattern10 = re.compile('^[0-9]+(_|\s)?(년)(_|\s)?(전기요금)(_|\()?(원)(_|\))?$')
    other_pattern11 = re.compile('^[0-9]+(_|\s)?(소비량)$')
    other_pattern12 = re.compile('^[0-9]+(_|\s)?(생산량)$')
    other_pattern13 = re.compile('^[0-9]+(년)(_|\s|\()?(귀촌)(_|\s|\))$')
    other_pattern14 = re.compile('^[0-9]+(년)(_|\s|\()?(귀농)(_|\s|\))$')
    other_pattern15 = re.compile('^[0-9]+(_|\s)?(년이용객)$')
    other_pattern16 = re.compile('^[0-9]+(년)(_|\s)?(전력사용량)(_|\()?(kWh)(_|\))?$')

    eng_pattern = re.compile('^[a-zA-Z]+$')
    
    for one_word in tqdm(result_word):

        word_id = one_word[0]
        ori_word = one_word[4]

        check_status = 'I'
        
        # 특수 기호 변경
        replace_hangeul = ori_word.replace(' ', '_')
        replace_hangeul = ori_word.replace('(', '_')
        replace_hangeul = replace_hangeul.replace(')', '_')
        
        replace_hangeul = replace_hangeul.replace('-', '_')
        replace_hangeul = replace_hangeul.replace(',', '_')
        replace_hangeul = replace_hangeul.replace('~', '_')
        replace_hangeul = replace_hangeul.replace('&', '_')
        replace_hangeul = replace_hangeul.replace('%', '_')

        replace_hangeul = replace_hangeul.replace('.', '')
        
        # 년도(4단어)로만 구성
        if year_pattern1.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년', '')

            replace_hangeul = 'Y_' + replace_hangeul
            check_status = 'E'

        if year_pattern1.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = 'M_' + replace_hangeul
            check_status = 'E'

        if year_pattern3.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = 'D_' + replace_hangeul
            check_status = 'E'

        if year_pattern4.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년', '')

            replace_hangeul = 'Y_' + replace_hangeul
            check_status = 'E'
        


        # 숫자로만 구성된 경우
        if num_pattern.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = 'E_' + replace_hangeul
            check_status = 'E'

        # 시간인 경우
        if hour_pattern1.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('시', '_')
            replace_hangeul = replace_hangeul.replace('-', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'H_' + replace_hangeul
            check_status = 'E'

        if hour_pattern2.match(replace_hangeul) != None and check_status == 'I' :
            replace_hangeul = replace_hangeul.replace('이전', '')
            replace_hangeul = replace_hangeul.replace('시', '_')
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'BE_H_' + replace_hangeul
            check_status = 'E'

        if hour_pattern3.match(replace_hangeul) != None and check_status == 'I' :
            replace_hangeul = replace_hangeul.replace('시', '_')
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'H_' + replace_hangeul
            check_status = 'E'

        # 월별
        if month_pattern.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = re.sub(pattern=r'^1월$', repl='JAN', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^2월$', repl='FEB', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^3월$', repl='MAR', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^4월$', repl='APR', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^5월$', repl='MAY', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^6월$', repl='JUN', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^7월$', repl='JUL', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^8월$', repl='AUG', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^9월$', repl='SEPT', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^10월$', repl='OCT', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^11월$', repl='NOV', string=replace_hangeul)
            replace_hangeul = re.sub(pattern=r'^12월$', repl='DEC', string=replace_hangeul)
            check_status = 'E'

        # 나이
        if age_pattern1.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_' + replace_hangeul
            check_status = 'E'

        if age_pattern2.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_' + replace_hangeul
            check_status = 'E'

        if age_pattern3.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('만', '')
            replace_hangeul = replace_hangeul.replace('세', '')
            replace_hangeul = replace_hangeul.replace('남자', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_M_' + replace_hangeul
            check_status = 'E'

        if age_pattern4.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('만', '')
            replace_hangeul = replace_hangeul.replace('세', '')
            replace_hangeul = replace_hangeul.replace('여자', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_W_' + replace_hangeul
            check_status = 'E'

        if age_pattern5.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace('에서', '_')
            replace_hangeul = replace_hangeul.replace('인구', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_' + replace_hangeul
            check_status = 'E'
        
        if age_pattern6.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace('이상', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_' + replace_hangeul + '_OVER'
            check_status = 'E'

        if age_pattern7.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('대', '')

            replace_hangeul = 'A_N_' + replace_hangeul
            check_status = 'E'

        if age_pattern8.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('이상', '')
            replace_hangeul = replace_hangeul.replace('미만', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_N_' + replace_hangeul
            check_status = 'E'

        if age_pattern9.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('이상', '')
            replace_hangeul = replace_hangeul.replace('이하', '')
            replace_hangeul = replace_hangeul.replace('중위수', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_N_MED_' + replace_hangeul
            check_status = 'E'

        if age_pattern10.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('세', '_')
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('이상', '')
            replace_hangeul = replace_hangeul.replace('이하', '')
            replace_hangeul = replace_hangeul.replace('평균', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'A_N_AVG_' + replace_hangeul
            check_status = 'E'

        # 인원
        if people_pattern1.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('인', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'P_' + replace_hangeul
            check_status = 'E'

        if people_pattern2.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('인', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'P_' + replace_hangeul
            check_status = 'E'

        if people_pattern3.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('명', '_')
            replace_hangeul = replace_hangeul.replace('사업체', '')
            replace_hangeul = replace_hangeul.replace('종사자수', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'P_E_' + replace_hangeul
            check_status = 'E'

        if company_pattern.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('명', '_')
            replace_hangeul = replace_hangeul.replace('사업체수', '')
            
            replace_hangeul = 'COM_' + replace_hangeul
            check_status = 'E'

        if grade_pattern.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('등급', '')
            
            replace_hangeul = 'G_' + replace_hangeul
            check_status = 'E'

        # 기타 패턴
        if other_pattern1.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('정원', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'FIX_NUM_' + replace_hangeul
            check_status = 'E'
        
        if other_pattern2.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('현원', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'CUR_NUM_' + replace_hangeul
            check_status = 'E'

        if other_pattern3.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('1차', '')
            replace_hangeul = 'F_' + replace_hangeul
            check_status = 'E'

        if other_pattern4.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('2차', '')
            replace_hangeul = 'S_' + replace_hangeul
            check_status = 'E'

        if other_pattern5.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('3차', '')
            replace_hangeul = 'T_' + replace_hangeul
            check_status = 'E'

        if other_pattern6.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('4차', '')
            replace_hangeul = 'F_' + replace_hangeul
            check_status = 'E'

        if other_pattern7.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('시', '_')
            replace_hangeul = replace_hangeul.replace('강우량', '')
            replace_hangeul = replace_hangeul.replace(' ', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'RAIN_' + replace_hangeul
            check_status = 'E'

        if other_pattern8.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('1st', 'FI_ST')
            replace_hangeul = replace_hangeul.replace('2nd', 'SE_ND')
            replace_hangeul = replace_hangeul.replace('3rd', 'TH_RD')
            replace_hangeul = replace_hangeul.replace('4th', 'FO_TH')
            replace_hangeul = replace_hangeul.replace('5th', 'FI_TH')
            replace_hangeul = replace_hangeul.replace('6th', 'SI_TH')
            replace_hangeul = replace_hangeul.replace('7th', 'SE_TH')
            replace_hangeul = replace_hangeul.replace('8th', 'EI_TH')

            check_status = 'E'

        if other_pattern9.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('종', '')

            replace_hangeul = 'SP_' + replace_hangeul
            check_status = 'E'

        if other_pattern10.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년', '_')
            replace_hangeul = replace_hangeul.replace('전기요금', '')
            replace_hangeul = replace_hangeul.replace('원', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'EL_CO_' + replace_hangeul
            check_status = 'E'

        if other_pattern11.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('소비량', '')
            
            replace_hangeul = 'CO_' + replace_hangeul
            check_status = 'E'


        if other_pattern12.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('생산량', '')

            replace_hangeul = 'CU_' + replace_hangeul
            check_status = 'E'

        if other_pattern13.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년', '_')
            replace_hangeul = replace_hangeul.replace('귀촌', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'RV_' + replace_hangeul
            check_status = 'E'

        if other_pattern14.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년', '_')
            replace_hangeul = replace_hangeul.replace('귀농', '')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'RF_' + replace_hangeul
            check_status = 'E'

        if other_pattern15.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년이용객', '')

            replace_hangeul = 'PA_' + replace_hangeul
            check_status = 'E'

        if other_pattern16.match(replace_hangeul) != None and check_status == 'I':
            replace_hangeul = replace_hangeul.replace('년', '_')
            replace_hangeul = replace_hangeul.replace('전력사용량', '')
            replace_hangeul = replace_hangeul.replace('kWh', '')
            replace_hangeul = replace_hangeul.replace('__', '_')
            replace_hangeul = replace_hangeul.replace('__', '_')

            replace_hangeul = 'EL_U_' + replace_hangeul
            check_status = 'E'

        # 영어
        if eng_pattern.match(replace_hangeul) != None and check_status == 'I':
            check_status = 'E'

        replace_hangeul = replace_hangeul.replace('__', '_')
        replace_hangeul = re.sub(pattern=r'^_', repl='', string=replace_hangeul)
        replace_hangeul = re.sub(pattern=r'_$', repl='', string=replace_hangeul)

        sql_update = """ UPDATE word_translate
                            SET word_hangeul_refined = ?,
                                check_status = ?
                            WHERE id = ?
                     """
        update_values = (replace_hangeul, check_status, word_id)
        cur_mariadb.execute(sql_update, update_values)
        conn_mariadb.commit()
        time.sleep(0.001)

    conn_mariadb.close()

    print('=========================================================================================')
    
