import sqlite3
import csv
import json


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def create_table_from_select_statement(conn,query,output_name_num):
    conn.execute('create table table_query' + output_name_num +  ' as ' + query)
    conn.commit()




def export_result_to_csv(rows,output_name_num):
    with open('data_csv' + output_name_num + '.csv', 'w', encoding='utf8') as fp:
        csvw = csv.writer(fp)
        csvw.writerows(rows)


def export_result_to_json(rows,output_file_query_number):
    with open('data_json' + output_file_query_number +'.json', 'w') as the_file:
        data = json.dumps(rows)
        print(data)
        the_file.write(data)



def connect_to_db(path_to_db):
    print('path to db is {}', path_to_db)
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = dict_factory
    c = conn.cursor()
    return c


def execute_queries(conn,query):
    conn.execute(query)
    rows = conn.fetchall()
    print(rows)
    #for row in conn:
        #print('{0} , {1}, {2} , {3} , {4} , {5}'.format(row[0], row[1], row[2], row[3], row[4], row[5]))
    return rows
    #with open('data.csv', 'w', encoding='utf8') as fp:
        #csvw = csv.writer(fp)
        #csvw.writerows(rows)
        # for row in c:
        #    print('{0} , {1}, {2} , {3} , {4} , {5}'.format(row[0], row[1], row[2], row[3], row[4], row[5]))


# for each song, get Song Name, Composer and its Genre.
def show_song_composer_genre():
    query = """
    SELECT tracks.name,
         composer,
         genres.name AS 'genre'
    FROM tracks
    LEFT JOIN genres
    ON tracks.genreId = genres.genreId
    """
    return query


def show_customers_info_and_number_of_purchases():
    query = """
    SELECT firstname,
        lastname,
        phone,
        email,
        coalesce(address,' ') ||
        coalesce(', ' || city,'') ||
        coalesce(', ' || state, '') ||
        coalesce(', ' || country, '') as full_address,
        count(invoiceid) AS 'purchases count'
    FROM customers
    JOIN invoices
        ON customers.customerid = invoices.customerid
    GROUP BY  customers.customerid
    """
    return query


def show_country_num_of_email_domains():
    query = """
    SELECT country,
        substr(e.email,
         instr(email,
         '@') + 1) AS domain, count(*)
    FROM customers e
    GROUP BY  substr(e.email, instr(email, '@') + 1)
    """
    return query


def show_country_purchased_albums():
    query = """
    SELECT DISTINCT country,
         count(albumid) AS 'number of purchased albums'
    FROM customers
    JOIN invoices
        ON customers.customerid = invoices.customerid
    JOIN invoice_items
        ON invoices.invoiceid = invoice_items.invoiceid
    JOIN tracks
        ON invoice_items.trackid = tracks.trackid
    GROUP BY  country
    """
    return query


def show_most_popular_album():
    query = """
    SELECT country,
         title,
        albumid from
    (SELECT count(albums.albumid) AS counter,
         albums.title AS title,
        country,
         albums.albumid
    FROM customers
    JOIN invoices
        ON customers.customerid = invoices.customerid
    JOIN invoice_items
        ON invoices.invoiceid = invoice_items.invoiceid
    JOIN tracks
        ON invoice_items.trackid = tracks.trackid
    JOIN albums
        ON tracks.albumid = albums.albumid
    GROUP BY  country,title
    ORDER BY  counter asc)
    GROUP BY  country
    """
    return query


def show_usa_most_popular_2011():
    query = """
    SELECT country,
         title,
        albumid,
        date from
    (SELECT count(albums.albumid) AS counter,
         albums.title AS title,
        country,
         albums.albumid,
        invoices.invoicedate AS date
    FROM customers
    JOIN invoices
        ON customers.customerid = invoices.customerid
    JOIN invoice_items
        ON invoices.invoiceid = invoice_items.invoiceid
    JOIN tracks
        ON invoice_items.trackid = tracks.trackid
    JOIN albums
        ON tracks.albumid = albums.albumid
    WHERE country = 'USA'
            AND invoices.invoicedate > '2010-12-12 00:00:00'
    GROUP BY  country,title
    ORDER BY  counter asc)
    GROUP BY  country
    """
    return query


def show_customer_with_two_nulls():
    query = """
    select * from
    customers
    join (SELECT   invoices.customerid,
         CASE WHEN billingcountry IS NOT NULL THEN 0 ELSE 1 END +
         CASE WHEN invoicedate IS NOT NULL THEN 0 ELSE 1 END +
         CASE WHEN billingaddress IS NOT NULL THEN 0 ELSE 1 END +
         CASE WHEN billingstate IS NOT NULL THEN 0 ELSE 1 END +
         CASE WHEN billingcity IS NOT NULL THEN 0 ELSE 1 END +
         CASE WHEN billingpostalcode IS NOT NULL THEN 0 ELSE 1 END AS 'number of null fields'
    FROM     invoices
    where `number of null fields` >= 2
    ) as a
    ON customers.customerId = a.customerid
    group by customers.customerid
    """
    return query