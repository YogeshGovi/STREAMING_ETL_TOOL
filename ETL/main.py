# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import subprocess
import os
import mysql.connector

con=mysql.connector.connect(host="localhost", user="admin", password="Yogovai!171290", port="3306", database="yogov")
res = con.cursor()
sql = "select distinct connection_name from yogov.connections"
res.execute(sql)
result = res.fetchall()
print(result)

def spark_start():
    # Use a breakpoint in the code line below to debug your script.
    # print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.
    # process = subprocess.Popen(['$SPARK_HOME/sbin/start-all.sh'],
    #                            stdout=subprocess.PIPE,
    #                            stderr=subprocess.PIPE)
    # stdout, stderr = process.communicate()
    # stdout, stderr

    os.system('$SPARK_HOME/sbin/start-all.sh')

def spark_stop():
    os.system('$SPARK_HOME/sbin/stop-all.sh')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    user_need=input("Enter what you need to do? ")
    user_need=user_need.upper()
    if(user_need=='START'):
        spark_start()
    if(user_need=='STOP'):
        spark_stop()



