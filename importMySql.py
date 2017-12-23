import json
import asyncio
import pymysql
import time
from aiomysql import create_pool

now = lambda: time.time()

async def exe_sql(item, lp):
	if len(item) < 9:
		return
	try:
		async with create_pool(host='59.78.45.122', port=3306,
							   user='sjh', password='123456',
							   db='sjhtest', loop=lp) as pool:
			async with pool.get() as conn:
				async with conn.cursor() as cur:
					await cur.execute("INSERT INTO video_games VALUES ('%s', '%s', '%s', '%s', '%s', %f, '%s', '%s', '%s')"
										%(str(item['reviewerID']), str(item['asin']),
										str(item['reviewerName']).replace("'", " ").replace('"', " "),str(item['helpful']),
										str(item['reviewText']).replace("'", " ").replace('"', " "),float(item['overall']),
										str(item['summary']).replace("'", " ").replace('"', " "),str(item['unixReviewTime']),
										str(item['reviewTime'])))
					# print (item['reviewerID'])
					await conn.commit()
	except KeyboardInterrupt:
		pass
	except:
		pass
	


def insertData(item):
	# print(len(item))
	if len(item) < 9:
		return
	db = pymysql.connect("59.78.45.122","sjh","123456","sjhtest" )
	cursor = db.cursor()
	try:
		cursor.execute("INSERT INTO video_games VALUES ('%s', '%s', '%s', '%s', '%s', %f, '%s', '%s', '%s')"
						%(str(item['reviewerID']), str(item['asin']),
						str(item['reviewerName']).replace("'", " ").replace('"', " "),str(item['helpful']),
						str(item['reviewText']).replace("'", " ").replace('"', " "),float(item['overall']),
						str(item['summary']).replace("'", " ").replace('"', " "),str(item['unixReviewTime']),
						str(item['reviewTime'])))
	except KeyboardInterrupt:
		exit()
	except:
		pass
	db.close()

def createTable():
	db = pymysql.connect("59.78.45.122","sjh","123456","sjhtest" )
	cursor = db.cursor()
	cursor.execute("CREATE TABLE video_games\
					(reviewerID varchar(255) not null, \
					asin varchar(255), \
					reviewerName varchar(255), \
					helpful varchar(255), \
					reviewText text, \
					overall double(20,10), \
					summary varchar(255), \
					unixReviewTime varchar(255), \
					reviewTime varchar(255), \
					primary key (reviewerID));")
	db.close()
	print ('create table done')

def loadData():
	STEP = 100
	logfile = open("{}_ins_log.csv".format(STEP), "w")
	count = 0
	f = open("reviews_Video_Games_5_array.json")

	data = json.load(f)
	print ('load data done', len(data))
	st = now()
	lst = st
	
	for sti in range(0, len(data), STEP):
		lp = asyncio.get_event_loop()
		tasks = [asyncio.ensure_future(exe_sql(item, lp)) for item in data[sti:sti+STEP]]
		# print (tasks)
		lp.run_until_complete(asyncio.wait(tasks))
		# count += len(dones)
		if sti % 10000 == 0:
			print ("{} Items Finished. Time Elapsed: {:.4}".format(sti+STEP, now() - st))
		logfile.write("{},{:.4}\n".format(sti+STEP, now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print ('All finished', now() - st)


createTable()
loadData()


