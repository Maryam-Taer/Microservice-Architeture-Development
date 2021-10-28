import mysql.connector

db_conn = mysql.connector.connect(host="acit3855-setc.eastus.cloudapp.azure.com", user="maryam", password="password", database="events")
db_cursor = db_conn.cursor()

db_cursor.execute('''
            DROP TABLE find_restaurant, write_review
          ''')

db_conn.commit()
db_conn.close()
