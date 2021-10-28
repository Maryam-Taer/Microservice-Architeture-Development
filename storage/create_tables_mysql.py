import mysql.connector

db_conn = mysql.connector.connect(host="acit3855-setc.eastus.cloudapp.azure.com", user="maryam", password="password", database="events")
db_cursor = db_conn.cursor()

db_cursor.execute(''' 
          CREATE TABLE find_restaurant
          (id INT NOT NULL AUTO_INCREMENT, 
           Restaurant_id VARCHAR(250) NOT NULL,
           Location VARCHAR(250) NOT NULL,
           Restaurant_type VARCHAR(100) NOT NULL,
           Delivery_option VARCHAR(100) NOT NULL,
           Open_on_weekends VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           CONSTRAINT blood_pressure_pk PRIMARY KEY (id))
          ''')

db_cursor.execute('''
          CREATE TABLE write_review
          (id INT NOT NULL AUTO_INCREMENT, 
           Post_id VARCHAR(250) NOT NULL,
           Username VARCHAR(250) NOT NULL,
           Rate_no INT NOT NULL,
           Review_description VARCHAR(100) NOT NULL,
           Time_posted VARCHAR(100) NOT NULL,
           date_created VARCHAR(100) NOT NULL,
           CONSTRAINT heart_rate_pk PRIMARY KEY (id))
          ''')

db_conn.commit()
db_conn.close()
