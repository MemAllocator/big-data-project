import pika
import json
from sqlite_connector import *
from xml.dom.minidom import parse,parseString

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='db_queue')

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    my_bytes_value = bytes(body)
    my_json = my_bytes_value.decode('utf8')
    payload = json.loads(my_json)
    print("db_path: {}".format(payload['db_path']))
    print("output_format: {}".format(payload['output_format']))
    c = connect_to_db(payload['db_path'])
    if payload['output_format'] == 'table':
        print('Generating and inserting Select Statements into DB')
        create_table_from_select_statement(c,show_song_composer_genre(),str(1))
        create_table_from_select_statement(c, show_customers_info_and_number_of_purchases(), str(2))
        create_table_from_select_statement(c, show_country_num_of_email_domains() ,str(3))
        create_table_from_select_statement(c, show_country_purchased_albums() ,str(4))
        create_table_from_select_statement(c, show_most_popular_album() ,str(5))
        create_table_from_select_statement(c, show_usa_most_popular_2011() ,str(6))
        create_table_from_select_statement(c, show_customer_with_two_nulls() ,str(7))

    else:
        print('Exporting queries results to output files')
        rows = execute_queries(c, show_song_composer_genre())
        #export_result_to_csv(rows)
        #export_result_to_json(rows, str(1))
        # for row in rows:
        #    print('{0} , {1}, {2} , {3} , {4} , {5}'.format(row[0], row[1], row[2], row[3], row[4], row[5]))
        # print(rows)
        var = payload['output_format']
        if var == 'csv':
            print("exporting queries in csv format")
            rows = execute_queries(c, query=show_song_composer_genre())
            export_result_to_csv(rows)
            rows = execute_queries(c, query=show_customers_info_and_number_of_purchases())
            export_result_to_csv(rows)

            rows = execute_queries(c, query=show_country_num_of_email_domains())
            export_result_to_csv(rows)

            rows = execute_queries(c, show_country_purchased_albums())
            export_result_to_csv(rows)

            rows = execute_queries(c, show_most_popular_album())
            export_result_to_csv(rows)

            rows = execute_queries(c, show_usa_most_popular_2011())
            export_result_to_csv(rows)

            rows = execute_queries(c, show_customer_with_two_nulls())
            export_result_to_csv(rows)
        elif var == 150:
            print
            "2 - Got a true expression value"

        elif var == 100:
            print
            "3 - Got a true expression value"
            print
            var
        else:
            print
            "4 - Got a false expression value"
            print
            var

        print
        "Good bye!"

channel.basic_consume(callback, queue='db_queue', no_ack=True)

channel.start_consuming()
