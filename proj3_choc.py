import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


def db_init():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
        DROP TABLE IF EXISTS Bars
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS Countries
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE IF NOT EXISTS Bars (
            Id INTEGER PRIMARY KEY,
            Company TEXT,
            SpecificBeanBarName TEXT,
            REF TEXT,
            ReviewDate TEXT,
            CocoaPercent REAL,
            CompanyLocation TEXT,
            CompanyLocationId INT,
            Rating REAL,
            BeanType TEXT,
            BroadBeanOrigin TEXT,
            BroadBeanOriginId INT
    );'''
    cur.execute(statement)

    statement = '''
        CREATE TABLE IF NOT EXISTS Countries (
            Id INTEGER PRIMARY KEY,
            Alpha2 TEXT,
            Alpha3 TEXT,
            EnglishName TEXT,
            Region TEXT,
            Subregion TEXT,
            Population INT,
            Area REAL
    );'''
    cur.execute(statement)

    conn.commit()
    conn.close()


def insert_bars_data():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    barscsv_infile = open(BARSCSV, 'r')
    barscsv_content = list(csv.reader(barscsv_infile))

    for row in barscsv_content[1:]:
        row[4] = float(row[4].replace('%', ''))/100
        statement = 'INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, ' \
                    'CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin) ' \
                    'VALUES ('+'?,'*8 + '?)'
        cur.execute(statement, row)

    conn.commit()
    conn.close()


def insert_countries_data():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    countriesjson_infile = open(COUNTRIESJSON, 'r')
    countries_pyobj = json.loads(countriesjson_infile.read())

    for i in countries_pyobj:
        statement = 'INSERT INTO Countries VALUES ('+'?,'*7 + '?)'
        insertion = (None, i['alpha2Code'], i['alpha3Code'], i['name'], i['region'], i['subregion'],
                     i['population'], i['area'])
        cur.execute(statement, insertion)

    conn.commit()
    conn.close()


def match_keys():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
    UPDATE Bars
    SET CompanyLocationId = (
        SELECT Id FROM Countries 
        WHERE Countries.EnglishName = Bars.CompanyLocation),
        BroadBeanOriginId = (
        SELECT Id FROM Countries
        WHERE Countries.EnglishName = Bars.BroadBeanOrigin)
    ;'''
    cur.execute(statement)

    conn.commit()
    conn.close()


# Part 2: Implement logic to process user commands
# param: a command string
# return: a list if tuples
def process_command(command):

    command_lst = command.strip().split()

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    if len(command_lst) > 0:
        main = command_lst[0]
        params = command_lst[1:]

        if main == 'bars':
            region = None
            region_name = None
            order = 'ratings'
            limit = 'top'
            num = 10

            for p in params:
                p_lst = p.split('=')
                p_key = p_lst[0]
                if p_key in ['sellcountry', 'sourcecountry', 'sellregion', 'sourceregion']:
                    region = p_key
                    try:
                        region_name = p_lst[1]
                    except:
                        return 'Missing value for parameters. Please try again.'
                elif p_key in ['ratings', 'cocoa']:
                    order = p_key
                elif p_key in ['top', 'bottom']:
                    limit = p_key
                    try:
                        num = p_lst[1]
                        if num <= 0:
                            return 'Invalid limit number "{}". Please try again.'.format(num)
                    except:
                        pass
                else:
                    return 'Invalid parameter "{}". Please try again.'.format(p_key)

            # order statement
            if order == 'ratings':
                order_column = 'Rating'
            else:
                order_column = 'CocoaPercent'

            if limit == 'top':
                order_direction = 'DESC'
            else:
                order_direction = 'ASC'

            # where statement
            if region == 'sellcountry':
                where_s = '''
                    JOIN Countries 
                        ON CompanyLocationId = Countries.Id
                    WHERE Alpha2 = '{}'
                '''.format(region_name)
            elif region == 'sourcecontry':
                where_s = '''
                    JOIN Countries 
                        ON Bars.BroadBeanOriginId = Countries.Id
                    WHERE Alpha2 = '{}'
                '''.format(region_name)
            elif region == 'sellregion':
                where_s = '''
                    JOIN Countries 
                        ON CompanyLocationId = Countries.Id
                    WHERE Region = '{}'
                '''.format(region_name)
            elif region == 'sourceregion':
                where_s = '''
                    JOIN Countries 
                        ON Bars.BroadBeanOriginId = Countries.Id
                    WHERE Region = '{}'
                '''.format(region_name)
            else:
                where_s = ''

            statement = '''
                SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, CocoaPercent, BroadBeanOrigin
                FROM Bars
                {}
                ORDER BY {} {}
                LIMIT {}
            '''.format(where_s, order_column, order_direction, num)

            cur.execute(statement)
            return_lst = cur.fetchall()

        elif main == 'companies':
            region = None
            region_name = None
            order = 'ratings'
            limit = 'top'
            num = 10

            for p in params:
                p_lst = p.split('=')
                p_key = p_lst[0]
                if p_key in ['country', 'region']:
                    region = p_key
                    try:
                        region_name = p_lst[1]
                    except:
                        return 'Missing value for parameters. Please try again.'
                elif p_key in ['ratings', 'cocoa', 'bars_sold']:
                    order = p_key
                elif p_key in ['top', 'bottom']:
                    limit = p_key
                    try:
                        num = p_lst[1]
                        if num <= 0:
                            return 'Invalid limit number "{}". Please try again.'.format(num)
                    except:
                        pass
                else:
                    return 'Invalid parameter "{}". Please try again.'.format(p_key)

            # order statement
            if order == 'ratings':
                order_criteria = 'AVG(Rating)'
            elif order == 'cocoa':
                order_criteria = 'AVG(CocoaPercent)'
            else:
                order_criteria = 'COUNT(*)'

            if limit == 'top':
                order_direction = 'DESC'
            else:
                order_direction = 'ASC'

            # having_and statement
            if region == 'country':
                having_s = 'AND Alpha2 = \'{}\''.format(region_name)
            elif region == 'region':
                having_s = 'AND Region = \'{}\''.format(region_name)
            else:
                having_s = ''

            statement = '''
                SELECT Company, CompanyLocation, {}
                FROM Bars
                    JOIN Countries
                        ON Bars.CompanyLocationId = Countries.Id
                GROUP BY Company
                HAVING COUNT(*) > 4 {}
                ORDER BY {} {}
                LIMIT {}
            '''.format(order_criteria, having_s, order_criteria, order_direction, num)

            cur.execute(statement)
            return_lst = cur.fetchall()

        elif main == 'countries':
            region = None
            region_name = None
            group_by = 'sellers'
            order = 'ratings'
            limit = 'top'
            num = 10

            for p in params:
                p_lst = p.split('=')
                p_key = p_lst[0]
                if p_key in ['region']:
                    region = p_key
                    try:
                        region_name = p_lst[1]
                    except:
                        return 'Missing value for parameters. Please try again.'
                elif p_key in ['sellers', 'sources']:
                    group_by = p_key
                elif p_key in ['ratings', 'cocoa', 'bars_sold']:
                    order = p_key
                elif p_key in ['top', 'bottom']:
                    limit = p_key
                    try:
                        num = p_lst[1]
                        if num <= 0:
                            return 'Invalid limit number "{}". Please try again.'.format(num)
                    except:
                        pass
                else:
                    return 'Invalid parameter "{}". Please try again.'.format(p_key)

            # order statement
            if order == 'ratings':
                order_criteria = 'AVG(Rating)'
            elif order == 'cocoa':
                order_criteria = 'AVG(CocoaPercent)'
            else:
                order_criteria = 'COUNT(*)'

            if limit == 'top':
                order_direction = 'DESC'
            else:
                order_direction = 'ASC'

            # having_and statement
            if region == 'region':
                having_s = 'AND Region = \'{}\''.format(region_name)
            else:
                having_s = ''

            # join statement
            if group_by == 'sellers':
                join_s = 'CompanyLocationId'
            else:
                join_s = 'BroadBeanOriginId'

            statement = '''
                SELECT Countries.EnglishName, Countries.Region, {}
                FROM Bars
                    JOIN Countries
                        ON Bars.{} = Countries.Id
                GROUP BY Countries.Alpha2
                HAVING COUNT(*) > 4 {}
                ORDER BY {} {}
                LIMIT {}
            '''.format(order_criteria, join_s, having_s, order_criteria, order_direction, num)

            cur.execute(statement)
            return_lst = cur.fetchall()

        elif main == 'regions':
            group_by = 'sellers'
            order = 'ratings'
            limit = 'top'
            num = 10

            for p in params:
                p_lst = p.split('=')
                p_key = p_lst[0]
                if p_key in ['sellers', 'sources']:
                    group_by = p_key
                elif p_key in ['ratings', 'cocoa', 'bars_sold']:
                    order = p_key
                elif p_key in ['top', 'bottom']:
                    limit = p_key
                    try:
                        num = p_lst[1]
                        if num <= 0:
                            return 'Invalid limit number "{}". Please try again.'.format(num)
                    except:
                        pass
                else:
                    return 'Invalid parameter "{}". Please try again.'.format(p_key)

            # order statement
            if order == 'ratings':
                order_criteria = 'AVG(Rating)'
            elif order == 'cocoa':
                order_criteria = 'AVG(CocoaPercent)'
            else:
                order_criteria = 'COUNT(*)'

            if limit == 'top':
                order_direction = 'DESC'
            else:
                order_direction = 'ASC'

            # join statement
            if group_by == 'sellers':
                join_s = 'CompanyLocationId'
            else:
                join_s = 'BroadBeanOriginId'

            statement = '''
                SELECT Region, {}
                FROM Bars
                    JOIN Countries
                        ON Bars.{} = Countries.Id
                GROUP BY Region
                HAVING COUNT(*) > 4
                ORDER BY {} {}
                LIMIT {}
            '''.format(order_criteria, join_s, order_criteria, order_direction, num)

            cur.execute(statement)
            return_lst = cur.fetchall()

        else:
            return "Command not recognized: " + command
    else:
        return ''

    conn.close()
    return return_lst


def load_help_text():
    with open('help.txt') as f:
        return f.read()


# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''

    while response != 'exit':
        response = input('\nEnter a command: ').strip()

        if response == 'help':
            print(help_text)
            continue
        elif response == 'exit':
            print('bye')
            break
        elif response == '':
            continue
        else:
            return_lst = process_command(response)
            if isinstance(return_lst, list):
                main = response.split()[0]
                if main in ['companies', 'countries', 'regions']:
                    agg = 'ratings'
                    try:
                        params = response.split()[1:]
                        for p in params:
                            p_lst = p.split('=')
                            p_key = p_lst[0]
                            if p_key in ['ratings', 'cocoa', 'bars_sold']:
                                agg = p_key
                    except:
                        pass

                    # <agg> part in output string
                    if agg == 'ratings':
                        agg_s = '1.1f'
                    elif agg == 'cocoa':
                        agg_s = '4.0%'
                    else:
                        agg_s = '4d'

                for row in return_lst:
                    row_lst = list(row)

                    # limit string length
                    for i, e in enumerate(row_lst):
                        if isinstance(e, str) and len(e) > 15:
                            row_lst[i] = e[:12] + '...'

                    if main == 'bars':
                        print('{0:15s} {1:15s} {2:15s} {3:1.1f} {4:4.0%} {5:15s}'.format(*row_lst))
                    elif main in ['companies', 'countries']:
                        output = '{0:15s} {1:15s} {2:' + agg_s + '}'
                        print(output.format(*row_lst))
                    else:
                        output = '{0:15s} {1:' + agg_s + '}'
                        print(output.format(*row_lst))
            else:
                print(return_lst)
                continue


# Make sure nothing runs or prints out when this file is run as a module
if __name__ == "__main__":
    db_init()
    insert_bars_data()
    insert_countries_data()
    match_keys()
    interactive_prompt()

