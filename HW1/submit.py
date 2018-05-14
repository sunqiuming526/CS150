import sqlite3
from prettytable import from_db_cursor

# copy and paste your SQL queries into each of the below variables
# note: do NOT rename variables

Q1 = '''
SELECT AVG(arr_delay) avg_delay
FROM flight_delays
'''

Q2 = '''
SELECT MAX(arr_delay) AS max_delay
FROM flight_delays
'''

Q3 = '''
SELECT D.carrier, D.fl_num, D.origin_city_name, D.dest_city_name, D.fl_date
FROM flight_delays D
WHERE D.arr_delay = (SELECT MAX(B.arr_delay)
    FROM flight_delays B)
'''

Q4 = '''
SELECT Weekday_name as weekday_name, D as avg_delay
FROM(
SELECT W.weekday_name,AVG(F.arr_delay) D
    FROM flight_delays F, weekdays W
    WHERE F.day_of_week = W.weekday_id
    GROUP BY F.day_of_week
)Weekday_delay
ORDER BY Weekday_delay.D DESC
'''

Q5 = '''
SELECT airline_name, avg_delay
FROM(
    SELECT A.airline_name , AVG(F.arr_delay) avg_delay
    FROM(SELECT *
         FROM(
        SELECT DISTINCT airline_id
        FROM flight_delays
        WHERE origin = "SFO") as C, flight_delays as D
         WHERE C.airline_id = D.airline_id
    ) F, airlines A
    WHERE F.airline_id = A.airline_id 
    GROUP BY F.airline_id
) D
ORDER BY avg_delay DESC


'''

Q6 = '''
SELECT B.b/A.a AS late_proportion
    FROM
    (SELECT COUNT(*)*1.0 a
    FROM(
     SELECT airline_id
     FROM flight_delays  
     GROUP BY airline_id
    )) A
    ,
    (SELECT COUNT(*)*1.0 b
    FROM(
     SELECT airline_id
     FROM flight_delays  
     GROUP BY airline_id
     HAVING AVG(arr_delay) > 10
    )) B
'''

Q7 = '''
SELECT su.s/tot.cont AS cov
FROM
    (SELECT SUM(cov.x * cov.y) s
    FROM(
        SELECT A.dd-B.dd AS x, A.ad-B.ad AS y
        FROM (SELECT dep_delay dd, arr_delay ad
              FROM flight_delays
             ) AS A,
                (SELECT AVG(dep_delay) dd, AVG(arr_delay) ad 
                FROM flight_delays 
               ) AS B) AS cov
    )AS su,(
    SELECT COUNT(*) cont
    FROM flight_delays
    ) AS tot


'''

Q8 = '''
SELECT airlines.airline_name, inc.increase AS delay_increase
FROM(
    SELECT bef.airline_id AS ids, (aft.delay-bef.delay) increase
    FROM(
        SELECT airline_id ,AVG(arr_delay) delay
        FROM flight_delays
        WHERE day_of_month > 23
        GROUP BY airline_id
        ) aft,(
        SELECT airline_id,AVG(arr_delay) delay
        FROM flight_delays
        WHERE day_of_month < 24
        GROUP BY airline_id
        ) bef
    WHERE bef.airline_id = aft.airline_id 
    ORDER BY increase DESC
    LIMIT 1
) inc, airlines
WHERE inc.ids = airlines.airline_id

'''

Q9 = '''
SELECT A.airline_name
FROM (
    SELECT DISTINCT airline_id id
    FROM flight_delays
    WHERE origin = "SFO" and dest = "EUG"
    INTERSECT
    SELECT DISTINCT airline_id
    FROM flight_delays
    WHERE origin = "SFO" and dest = "PDX"
) B, airlines A
WHERE B.id = A.airline_id
ORDER BY B.id DESC
'''

Q10 = '''
SELECT origin, dest ,AVG(arr_delay) as avg_delay
FROM flight_delays
WHERE (origin = "MDW" or origin = "ORD") and (dest = "SFO" or dest = "SJC" or dest = "OAK") and crs_dep_time > 1400
GROUP BY origin, dest
ORDER BY avg_delay DESC

'''

#################################
# do NOT modify below this line #
#################################

# open a database connection to our local flights database
def connect_database(database_path):
    global conn
    conn = sqlite3.connect(database_path)

def get_all_query_results(debug_print = True):
    all_results = []
    for q, idx in zip([Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10], range(1, 11)):
        result_strings = ("The result for Q%d was:\n%s\n\n" % (idx, from_db_cursor(conn.execute(q)))).splitlines()
        all_results.append(result_strings)
        if debug_print:
            for string in result_strings:
                print string
    return all_results

if __name__ == "__main__":
    connect_database('flights.db')
    query_results = get_all_query_results()