from flask import Flask,redirect,url_for,render_template,request
import mysql.connector
import findspark
import os
import time

app=Flask(__name__)

os.environ["SPARK_HOME"] = "/usr/local/spark/"
findspark.init()

con=mysql.connector.connect(host="localhost", user="admin", password="Yogovai!171290", port="3306", database="yogov")

if con:
    print("connected")
else:
    print("Not connected")


@app.route('/')
def welcome():
    return render_template('home.html')
    #return render_template('connection.html')

@app.route('/save/<conn_name>/<conn_type>/<uname>/<pwd>/<hostname>/<port>/<sid>',methods=['POST','GET'])
def save(conn_name,conn_type,uname,pwd,hostname,port,sid):
    res=con.cursor()
    sql="insert into yogov.connections values (%s,%s,%s,%s,%s,%s,%s)"
    conn_details=(conn_name,conn_type,uname,pwd,hostname,port,sid)
    res.execute(sql, conn_details)
    con.commit()
    result='inserted'
    return render_template('result.html')

@app.route('/fetch_connections')
def fetch_connections():
    res=con.cursor()
    sql="select distinct connection_name from yogov.connections"
    res.execute(sql)
    result=res.fetchmany()

@app.route('/connection', methods=['POST','GET'])
def connection():
    return render_template('connection.html')

@app.route('/submit',methods=['POST','GET'])
def submit():
    if request.method=='POST':
        con_name=request.form['connection_name']
        con_type = request.form['connection_type']
        username=request.form['user_name']
        pswd=request.form['password']
        hostnm=request.form['hostname']
        portno=request.form['port']
        sidd=request.form['sid']

    return redirect(url_for('save',conn_name=con_name,conn_type=con_type,uname=username,pwd=pswd,hostname=hostnm,port=portno,sid=sidd))

@app.route('/ETL', methods=['POST', 'GET'])
def ETL():
    res = con.cursor()
    sql = "select distinct connection_name from yogov.connections"
    res.execute(sql)

    result = res.fetchall()
    print(result)

    # cursor = con.cursor()
    # cur = cursor.execute("SELECT distinct connection_name FROM yogov.connections")
    return render_template('ETL.html', conn_name=result)

@app.route('/Execute', methods=['POST', 'GET'])
def execute():
    if request.method=='POST':
        src_name=request.form['src_name']
        source_tbl=request.form['source_tbl']
        print(src_name)
        print(source_tbl)
        res = con.cursor()
        src_sql = "select * from yogov.connections where connection_name=%s"
        val1 = [(src_name)]
        res.execute(src_sql,val1)
        src_result = res.fetchall()

        source_uname=src_result[0][2]
        source_pwd=src_result[0][3]
        source_host=src_result[0][4]
        source_port=src_result[0][5]
        source_sid=src_result[0][6]

        tgt_name = request.form['tgt_name']
        target_tbl = request.form['target_tbl']

        tgt_sql = "select * from yogov.connections where connection_name=%s"
        val2 = [(tgt_name)]
        res.execute(src_sql, val2)
        tgt_result = res.fetchall()


        target_uname = tgt_result[0][2]
        target_pwd = tgt_result[0][3]
        target_host = tgt_result[0][4]
        target_port = tgt_result[0][5]
        target_sid = tgt_result[0][6]

        print(tgt_name)
        print(target_tbl)
        con.commit()
    return redirect(url_for('migrate',src_uname=source_uname, src_pwd=source_pwd, src_host=source_host,
                            src_port=source_port, src_sid=source_sid, src_tbl=source_tbl,
                            tgt_uname=target_uname, tgt_pwd=target_pwd, tgt_host=target_host,
                            tgt_port=target_port, tgt_sid=target_sid, tgt_tbl=target_tbl))



@app.route('/migrate/<src_uname>/<src_pwd>/<src_host>/<src_port>/<src_sid>/<src_tbl>/'
           '<tgt_uname>/<tgt_pwd>/<tgt_host>/<tgt_port>/<tgt_sid>/<tgt_tbl>',methods=['POST','GET'])
def migrate(src_uname,src_pwd,src_host,src_port,src_sid,src_tbl,tgt_uname,tgt_pwd,tgt_host,tgt_port,tgt_sid,tgt_tbl):

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("spark://master:7077").appName("Read_20_GB_CSV").config("spark.executor.cores",
                                                                                                "4").config(
        "spark.driver.memory", "2g").config("spark.executor.memory", "2g")\
        .config('spark.jars','/home/hduser/PycharmProjects/ETL/jars/mysql-connector-java-8.0.30.jar, /home/hduser/PycharmProjects/ETL/jars/ojdbc8-19.3.0.0.jar')\
        .getOrCreate()

    #.config('spark.jars', '/home/hduser/Documents/Database_Details/RDBMS/ojdbc8-19.3.0.0.jar') \
    # .config('spark.jars.packages', '/home/hduser/Documents/Database Details/RDBMS/mysql-connector-java-8.0.30')\

    oracle_jdbc_url = "jdbc:oracle:thin:@"+src_host +":"+ src_port +'/'+src_sid
    mysql_jdbc_url="jdbc:mysql://"+tgt_host+":"+tgt_port+'/'+tgt_sid


    oracle_jdbc_driver="oracle.jdbc.driver.OracleDriver"
    mysql_jdbc_driver="com.mysql.jdbc.Driver"


    print(mysql_jdbc_url)
    print(oracle_jdbc_url)


    src_df = spark.read.format("jdbc")\
        .option("url", oracle_jdbc_url) \
        .option("driver", oracle_jdbc_driver)\
        .option("dbtable", src_tbl) \
        .option("user", src_uname)\
        .option("password", src_pwd) \
        .option("partitionColumn", "OBJECT_ID")\
        .option("lowerBound", "100")\
        .option("upperBound", "1000")\
        .option("numPartitions", "1000")\
        .load()

    # .option("driver", "com.mysql.jdbc.Driver")\

    src_df.printSchema()
    src_df=src_df.repartition(1000)
    print("Repartition Completed ...")
    print(src_df.count())

    #src_df.createOrReplaceTempView("src_vw")
    #src_df_count=spark.sql("select count(1) from src_vw")
    #print(src_df_count.show())

    tgt_df=src_df.select("*")\
        .write\
        .format("jdbc")\
        .option("url", mysql_jdbc_url) \
	    .option("driver", mysql_jdbc_driver)\
        .option("dbtable", tgt_tbl) \
	    .option("user", tgt_uname)\
        .option("password", tgt_pwd) \
        .option("partitionColumn", "OBJECT_ID") \
        .option("lowerBound", "100") \
        .option("upperBound", "1000") \
        .option("numPartitions", "1000")\
        .mode('append')\
        .save()



    return "Data Migration Completed"


if __name__=='__main__':
    app.run(debug=True)