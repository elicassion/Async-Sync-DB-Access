import json
import asyncio
import pymysql
import time
from aiomysql import create_pool

now = lambda: time.time()
DATALEN = 231780
STEP = 10
SYNC = True

async def exe_sql(item, lp):
	# if len(item) < 9:
	# 	print (item)
	if not 'reviewerName' in item.keys():
		item['reviewerName'] = ""
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
	except Exception as e:
		print (e)
		# print (item)
	




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
					reviewTime varchar(255));")
	db.close()
	print ('Create Table Done')


def countDb():
	db = pymysql.connect("59.78.45.122","sjh","123456","sjhtest" )
	cursor = db.cursor()
	cursor.execute("select count(*) from video_games;")
	ct = cursor.fetchone()[0]
	# print (ct[0])
	db.close()
	print ('Succsessfully Inserted {}/{} Rate: {:.4}'.format(ct, DATALEN, ct/DATALEN))
	if SYNC:
		logfile = open("{}_{}_ins_log_s.csv".format(STEP, DATALEN), "a")
	else:
		logfile = open("{}_{}_ins_log_a.csv".format(STEP, DATALEN), "a")
	logfile.write("{},{},{:.3}".format(ct, DATALEN, ct/DATALEN))


def dropDb():
	db = pymysql.connect("59.78.45.122","sjh","123456","sjhtest" )
	cursor = db.cursor()
	cursor.execute("drop table video_games;")
	db.commit()
	print ('Clear!')

def insertDbSync():
	logfile = open("{}_{}_ins_log_s.csv".format(STEP, DATALEN), "w")
	count = 0
	f = open("reviews_Video_Games_5_array.json")

	data = json.load(f)
	print ('Load Data Done.', len(data))
	st = now()
	lst = st
	
	for i, item in enumerate(data):
		if not 'reviewerName' in item.keys():
			item['reviewerName'] = ""
		try:
			db = pymysql.connect("59.78.45.122","sjh","123456","sjhtest" )
			cursor = db.cursor()
			cursor.execute("INSERT INTO video_games VALUES ('%s', '%s', '%s', '%s', '%s', %f, '%s', '%s', '%s')"
											%(str(item['reviewerID']), str(item['asin']),
											str(item['reviewerName']).replace("'", " ").replace('"', " "),str(item['helpful']),
											str(item['reviewText']).replace("'", " ").replace('"', " "),float(item['overall']),
											str(item['summary']).replace("'", " ").replace('"', " "),str(item['unixReviewTime']),
											str(item['reviewTime'])))
			db.commit()
		except KeyboardInterrupt:
			pass
		except Exception as e:
			print (e)
		db.close()
		if (i+1) % 10000 == 0:
			print ("{} Items Finished. Time Elapsed: {:.4}".format((i+1), now() - lst))
			logfile.write("{},{:.4}\n".format((i+1), now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print ('All finished', now() - st)
	logfile.close()


def insertDbAsync():
	logfile = open("{}_{}_ins_log_a.csv".format(STEP, DATALEN), "w")
	count = 0
	f = open("reviews_Video_Games_5_array.json")

	data = json.load(f)
	print ('Load Data Done.', len(data))
	st = now()
	lst = st
	
	for sti in range(0, DATALEN, STEP):
		lp = asyncio.get_event_loop()
		tasks = [asyncio.ensure_future(exe_sql(item, lp)) for item in data[sti:sti+STEP]]
		# print (tasks)
		lp.run_until_complete(asyncio.wait(tasks))
		# count += len(dones)
		if (sti + STEP) % 1000 == 0:
			print ("{} Items Finished. Time Elapsed: {:.4}".format(sti+STEP, now() - st))
			logfile.write("{},{:.4}\n".format(sti+STEP, now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print ('All finished', now() - st)
	logfile.close()


createTable()
if SYNC:
	insertDbSync()
else:
	insertDbAsync()
countDb()
# dropDb()



