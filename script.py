import vk_api
import time
import psycopg2
import datetime

last_vk_request = 0
ct = 0

conn = None
vk_session = None
vk = None

def waiter(interval):
    global ct, last_vk_request
    ct = int(time.time())
    
    if ct - last_vk_request < interval:
        q = interval - (ct - last_vk_request)
        print("Inter-query cooldown being held, please wait %02d" % q, end='')
        for _ in range(q):
            time.sleep(1)
            print("\b\b%02d" % (q - _), end='')
        print("\r                                                                                              \r" , end='')
    last_vk_request = ct

def swaiter(interval):
    global ct, last_vk_request
    ct = int(time.time())
    
    if ct - last_vk_request < interval:
        q = interval - (ct - last_vk_request)
        for _ in range(q):
            time.sleep(1)
    last_vk_request = ct

def ecranize(a: str) -> str:
    return a.replace('\'', "\'\'")


def prepare():
    global conn, vk_session, vk
    print("GREAT CHALLENGES STRATEGY VK-SEARCH BOT\nMICHAEL YARIKOV (MichYar) 2023.\n\n")
    conn = psycopg2.connect(database="DATABASE", user="USER", password="PASSWORD", host="HOST", port="PORT")
    print("\nDATABASE: CONNECTED")

    vk_session = vk_api.VkApi('PHONENUMBER', 'PASSWORD NUMBER TWO')
    print("VK API:   CONNECTED")
    vk_session.auth()
    vk = vk_session.get_api()

prepare()

t = []
global_city_set = set()
global_country_set = set()

with conn.cursor() as c1:
    query = '''
        SELECT
            max(id)
        FROM
            vk_fetch
    '''
    c1.execute(query)
    incrementer = ( c1.fetchone()[0] or 0 ) + 1 


    query = '''
		SELECT
            max(id)
		FROM
            vk_edu
        '''
    c1.execute(query)
    vk_edu_id = ( c1.fetchone()[0] or 0 ) + 1 

    query = '''
		SELECT
            max(id)
		FROM
		    vk_sch_edu
        '''
    c1.execute(query)
    vk_sch_edu_id = ( c1.fetchone()[0] or 0 ) + 1 

    c1.execute('''
                SELECT DISTINCT
                        f_name, s_name, l_name, vk_uid
                FROM
                    people_src_20230219
                ORDER BY
                    l_name
                LIMIT 100
               ''')
    rows_total = c1.rowcount
    cur_record = 0
    for entry in c1.fetchall():
        cur_record += 1
        res = []
        vk_response = {}

        if entry[3] is not None:
            print('QUERY:', entry[3])
            waiter(5)
            qqq = vk.users.search(
                q = entry[3],
                fields = 'bdate, education, universities, schools, city, screen_name'
            )
            if len(qqq['items']) > 0:
                vk_response.update(qqq)
        if (len(qqq['items']) == 0 ) or (entry[3] is None):
            print('QUERY:', entry[2] + ' ' + entry[0] + ' with hometown Липецк')

            waiter(5)
            vk_response.update(
                vk.users.search(
                    q = entry[2] + ' ' + entry[0],
                    fields = 'bdate, education, universities, schools, city, screen_name', 
                    sort=0, 
                    count=1000, 
                    hometown = 'Липецк'
                )
            )
            print('QUERY:', entry[2] + ' ' + entry[0] + ' with city Липецк')

            waiter(5)
            vk_response.update (
                vk.users.search(
                    q = entry[2] + ' ' + entry[0],
                    fields = 'bdate, education, universities, schools, city, screen_name', 
                    sort=0, 
                    count=1000, 
                    city = 78
                )
            )
            print('QUERY:', entry[2] + ' ' + entry[0] + ' globally')

            waiter(5)
            vk_response.update (
                vk.users.search(
                    q = entry[2] + ' ' + entry[0],
                    fields = 'bdate, education, universities, schools, city, screen_name', 
                    sort=0, 
                    count=1000
                )
            )
        iterator = 0
        res_p = {}
        res_u = {}
        res_sch = {}
        res_u_f = {}
        res_city = set()
        res_country = set()
        res_edu = {}
        res_sch_edu = {}
        for entry_ in vk_response['items']:
            res_p[entry_['id']] = {}
            res_p[entry_['id']]['is_closed'] = entry_['is_closed']
            res_p[entry_['id']]['vk_uid'] = entry_['id']
            res_p[entry_['id']]['vk_screen_name'] = entry_['screen_name']
            res_p[entry_['id']]['birthday'] = datetime.datetime.strptime('.'.join([x.zfill(2) for x in entry_['bdate'].split('.')]), '%d.%m.%Y')  if 'bdate' in entry_ and len(entry_['bdate'].split('.')) == 3 else None
            res_p[entry_['id']]['l_name'] = entry_['last_name']
            res_p[entry_['id']]['f_name'] = entry_['first_name']
            res_p[entry_['id']]['city'] = entry_['city']['title'] if 'city' in entry_ and 'title' in entry_['city'] else None
            res_p[entry_['id']]['bday'] = entry_['bdate'].split('.')[0] if 'bdate' in entry_ else None
            res_p[entry_['id']]['bmonth'] = entry_['bdate'].split('.')[1] if 'bdate' in entry_ else None
            res_p[entry_['id']]['byear'] = entry_['bdate'].split('.')[2] if 'bdate' in entry_ and len(entry_['bdate'].split('.')) > 2 else None
            res_p[entry_['id']]['search_key'] = entry[2] + ' ' + entry[0]
            
            if 'universities' in entry_:
               #parse Universities here
               for cur_uni in entry_['universities']:
                    res_u[cur_uni['id']] = {}
                    country_t = cur_uni['country'] if cur_uni['country'] > 0 else cur_uni['country'] * -1
                    res_u[cur_uni['id']]['country_id'] = country_t
                    city_t = cur_uni['city'] if cur_uni['city'] > 0 else cur_uni['city'] * -1
                    res_u[cur_uni['id']]['city_id'] = city_t
                    res_u[cur_uni['id']]['name'] = cur_uni['name']
                    if str(city_t) not in global_city_set:
                        res_city.add(str(city_t))
                    if str(country_t) not in global_country_set:
                        res_country.add(str(country_t))
                   
                    if 'faculty' in cur_uni:
                        res_u_f[cur_uni['faculty']] = {}
                        res_u_f[cur_uni['faculty']]['uni_id'] = cur_uni['id']
                        res_u_f[cur_uni['faculty']]['name'] = cur_uni['faculty_name']

                    res_edu[vk_edu_id] = {}
                    res_edu[vk_edu_id]['vk_uid'] = entry_['id']
                    res_edu[vk_edu_id]['uni_id'] = cur_uni['id']
                    res_edu[vk_edu_id]['faculty_id'] = cur_uni['faculty'] if 'faculty' in cur_uni else None
                    res_edu[vk_edu_id]['chair_name'] = cur_uni['chair_name'] if 'chair_name' in cur_uni else None
                    vk_edu_id += 1

            if 'schools' in entry_:
                for cur_sch in entry_['schools']:
                    #parse Schools here
                    city_t = cur_sch['city'] if cur_sch['city'] > 0 else cur_sch['city'] * -1
                    country_t = cur_sch['country']  if cur_sch['country'] > 0 else cur_sch['country'] * -1
                    if str(city_t) not in global_city_set:
                        res_city.add(str(city_t))
                    if str(country_t) not in global_country_set:
                        res_country.add(str(country_t))
                    res_sch[cur_sch['id']] = {}
                    res_sch[cur_sch['id']]['name'] = cur_sch['name']
                    res_sch[cur_sch['id']]['city_id'] = city_t
                    res_sch[cur_sch['id']]['country_id'] = country_t

                    res_sch_edu[vk_sch_edu_id] = {}
                    res_sch_edu[vk_sch_edu_id]['vk_uid'] = entry_['id']
                    res_sch_edu[vk_sch_edu_id]['sch_id'] = cur_sch['id']
                    vk_sch_edu_id += 1
        global_country_set.update(res_country)
        global_city_set.update(res_city)
        #end of lookup process for 1 search_key

        #DB Update section

        if len(res_country):
            query_for_countries = ','.join(res_country)
            print('QUERY: Countries from VK')
            waiter(5)
            c_country = vk.database.getCountriesById(country_ids = query_for_countries)
            print('INSERT: Countries in DB')
            with conn.cursor() as c2:
                #Countries update
                query = '''
                    INSERT INTO vk_country
                        (id, name)
                    VALUES(0,'Не определено') on conflict do nothing
                '''
                c2.execute(query)

                for cc in c_country:
                    query = '''
                        INSERT INTO vk_country
                            (id, name)
                        VALUES(%s,%s) on conflict do nothing
                    '''
                    c2.execute(
                        query,
                        (
                            cc.get('id'),
                            cc.get('title')
                        )
                    )
            conn.commit()

        if len(res_city):
            query_for_cities = ','.join(res_city)
            print('QUERY: Cities from VK')
            waiter(5)
            c_city = vk.database.getCitiesById(city_ids = query_for_cities)
            print('INSERT: Cities in DB')
            with conn.cursor() as c2:
                #Cities update
                query = '''
                    INSERT INTO vk_city
                        (id, name)
                    VALUES(0,'Не определено') on conflict do nothing
                '''
                c2.execute(query)

                for cc in c_city:
                    query = '''
                        INSERT INTO vk_city
                            (id, name)
                        VALUES(%s,%s) on conflict do nothing
                    '''
                    c2.execute(
                        query,
                        (
                            cc.get('id'), 
                            cc.get('title')
                        )
                    )
            conn.commit()

        if len(res_sch):
            print('INSERT: Schools in DB')
            with conn.cursor() as c2:
                for cur_sch in res_sch:
                    #Inserting schools in DB
                    query = '''
                        INSERT INTO vk_school
                            (id, city_id, country_id, name)
                        VALUES(%s, %s, %s, %s) on conflict do nothing
                    '''
                    c2.execute(
                        query,
                        (
                            cur_sch, 
                            res_sch[cur_sch].get('city_id'), 
                            res_sch[cur_sch].get('country_id'), 
                            res_sch[cur_sch].get('name')
                        )
                    )
            conn.commit()
        
        if len(res_u):
            print('INSERT: Universities in DB')
            with conn.cursor() as c2:
                for cur_uni in res_u:
                    #Inserting universities in DB
                    query = '''
                        INSERT INTO vk_uni
                            (id, city_id, country_id, name)
                        VALUES(%s, %s, %s, %s) on conflict do nothing
                    '''

                    c2.execute(
                        query,
                        (
                            cur_uni, 
                            res_u[cur_uni].get('city_id'), 
                            res_u[cur_uni].get('country_id'), 
                            res_u[cur_uni].get('name')
                        )
                    )
            conn.commit()

        if len(res_u_f):
            print('INSERT: Faculties in DB')
            with conn.cursor() as c2:
                for cur_uni_f in res_u_f:
                    #Inserting faculties in DB
                    query = '''
                        INSERT INTO vk_uni_faculty
                            (id, uni_id, name)
                        VALUES(%s, %s, %s) on conflict do nothing
                    '''
                    c2.execute(
                        query,
                        (
                            cur_uni_f, 
                            res_u_f[cur_uni_f].get('uni_id'), 
                            res_u_f[cur_uni_f].get('name')
                        )
                    )
            conn.commit()

        if len(res_p):
            print('INSERT: People in DB')
            with conn.cursor() as c2:
                for cur_p in res_p:
                    #Inserting VK found people in DB
                    query = """
                        INSERT INTO vk_fetch
                            (id, is_closed, birthday, l_name, f_name, hometown, bday, bmonth, byear, search_key, vk_uid, vk_screen_name)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing
                    """
                    c2.execute(
                        query,
                        (
                            incrementer,
                            res_p[cur_p].get('is_closed'),
                            res_p[cur_p].get('birthday').strftime('%Y-%m-%d') if res_p[cur_p].get('birthday') is not None else None,
                            res_p[cur_p].get('l_name'),
                            res_p[cur_p].get('f_name'),
                            res_p[cur_p].get('city'),
                            res_p[cur_p].get('bday'),
                            res_p[cur_p].get('bmonth'),
                            res_p[cur_p].get('byear'),
                            res_p[cur_p].get('search_key'),
                            res_p[cur_p].get('vk_uid'),
                            res_p[cur_p].get('vk_screen_name')
                        )
                    )
                    incrementer += 1
            conn.commit()

        if len(res_sch_edu):
            print('INSERT: School education in DB')
            with conn.cursor() as c2:
                for cur_s_e in res_sch_edu:
                    #Inserting school education DB
                    query = '''
                        INSERT INTO vk_sch_edu
                            (id, vk_uid, sch_id)
                        VALUES(%s, %s, %s) on conflict do nothing
                    '''
                    c2.execute(
                        query,
                        (
                            cur_s_e,
                            res_sch_edu[cur_s_e].get('vk_uid'),
                            res_sch_edu[cur_s_e].get('sch_id')
                        )
                    )
            conn.commit()

        if len(res_edu):
            print('INSERT: University education in DB')
            with conn.cursor() as c2:
                for cur_e in res_edu:
                    #Inserting University education DB
                    query = '''
                        INSERT INTO vk_edu
                            (id, vk_uid, faculty_id, uni_id, chair_name)
                        VALUES(%s, %s, %s, %s, %s) on conflict do nothing
                    '''
                    c2.execute(
                        query,
                        (
                            cur_e, 
                            res_edu[cur_e].get('vk_uid'), 
                            res_edu[cur_e].get('faculty_id'), 
                            res_edu[cur_e].get('uni_id'), 
                            res_edu[cur_e].get('chair_name')
                        )
                    )
            conn.commit()

        print('Insertion successful! %06.2f%%\n---------------------------------------------------------' % (cur_record * 100 / rows_total) )        

    print('QUERY: Fetching Strategy48 members:      ', end='')

    t = []
    offset_num = 0
    while True:
        t = vk.groups.getMembers(group_id = 'club127973328',
                sort = 'id_asc', 
                offset = offset_num,
                count=500
            )
        query = """
            UPDATE vk_fetch
            SET strategy48_member = true
            WHERE vk_uid IN (%s)
        """ % ','.join(map(str, t['items']))

        c1.execute(query)
        if len(t['items']) < 500:
            print ('\b\b\b\bDone.')
            break;
        else:
            print ('\b\b\b\b\b%5d' % (len(t['items']) + offset_num) , end = '')
            offset_num += 500
            swaiter(5)

    waiter(5)

    print('QUERY: Fetching Kvantorium48 members:      ', end='')
    t = []
    offset_num = 0
    while True:
        t = vk.groups.getMembers(group_id = 'club141995075',
                sort = 'id_asc', 
                offset = offset_num,
                count=500
            )
        query = """
            UPDATE vk_fetch
            SET kvantorium48_member = true
            WHERE vk_uid IN (%s)
        """ % ','.join(map(str, t['items']))

        c1.execute(query)
        if len(t['items']) < 500:
            print ('\b\b\b\bDone.')
            break;
        else:
            print ('\b\b\b\b\b%5d' % (len(t['items']) + offset_num) , end = '')
            offset_num += 500
            swaiter(5)
    conn.commit()
print ("Ok")

    
