import pymysql, time

# 打开数据库连接
st = time.time()
db = pymysql.connect("localhost","root","123456","dbtest" )
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
 
# 使用 execute()  方法执行 SQL 查询 
for i in range(10):
	cursor.execute("SELECT 42;")
 
# 使用 fetchone() 方法获取单条数据.
	data = cursor.fetchone()
 
	print (data)
	time.sleep(1)
 
# 关闭数据库连接
db.close()
print (time.time() - st)