import json
import asyncio
import pymysql
import time
import random
from aiomysql import create_pool

now = lambda: time.time()
DATALEN = 231780
REPEATTIME = 200
STEP = 8
SYNC = False

HOST = 'localhost'
USER = 'root'
PORT = 3306
PW = '123456'
DB = 'sjhtest'

COUNT = [0 for i in range(STEP)]

async def exe_sql(lp, i):
	try:
		async with create_pool(host=HOST, port=PORT,
							   user=USER, password=PW,
							   db=DB, loop=lp) as pool:
			async with pool.get() as conn:
				async with conn.cursor() as cur:
					await cur.execute("SELECT reviewerID, reviewTime, reviewText, summary \
										FROM review \
										WHERE reviewTime LIKE '%{}';"
										.format(random.randint(2000, 2014)))
					# print (item['reviewerID'])
					# await conn.commit()
					value = await cur.fetchall()
					COUNT[i] += len(value)
	except KeyboardInterrupt:
		pass
	except Exception as e:
		print (e)
		# print (item)

async def ins_sql(lp):
	try:
		async with create_pool(host=HOST, port=PORT,
							   user=USER, password=PW,
							   db=DB, loop=lp) as pool:
			async with pool.get() as conn:
				async with conn.cursor() as cur:
					await cur.execute("INSERT INTO review VALUES \
										('TESTUSER', {}, {}, {}, {}, {}, {}, {});"
										.format(random.randint(0,100), random.randint(0,100), random.randint(0,100), 
											random.randint(0,100), random.randint(0,100), random.randint(0,100), 
											random.randint(2000,2014)))
					# print (item['reviewerID'])
					await conn.commit()
	except KeyboardInterrupt:
		pass
	except Exception as e:
		print (e)
		# print (item)


def createTable():
	db = pymysql.connect(HOST,USER,PW,DB )
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
	db = pymysql.connect(HOST,USER,PW,DB )
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
	db = pymysql.connect(HOST,USER,PW,DB )
	cursor = db.cursor()
	cursor.execute("drop table video_games;")
	db.commit()
	print ('Clear!')

def selectDbSync():
	logfile = open("{}_{}_sel_log_s.csv".format(REPEATTIME, STEP), "w")
	count = 0

	st = now()
	lst = st
	print (STEP, 'Task Sync Test Start')
	db = pymysql.connect(HOST,USER,PW,DB )
	cursor = db.cursor()
	for i in range(REPEATTIME):
		try:
			cursor.execute("SELECT reviewerID, reviewTime, reviewText, summary \
							FROM review \
							WHERE reviewTime LIKE '%{}';"
							.format(random.randint(2000, 2014)))
			# db.commit()
			value = cursor.fetchall()
			count += len(value)
		except KeyboardInterrupt:
			pass
		except Exception as e:
			print (e)
		# db.close()
		if (i+1) % STEP == 0:
			# print ("{} Queries Finished, {} Lines Fetched. Time Elapsed: {:.4}".format((i+1), count, now() - st))
			logfile.write("{},{},{:.4}\n".format((i+1), count, now()-lst))
			# print ("{} Queries Finished. Time Elapsed: {:.4}".format((i+1), now() - st))
			# logfile.write("{},{:.4}\n".format((i+1), now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print (STEP, 'Task Sync Test Finished', now() - st)
	logfile.close()


def selectDbAsync():
	# logfile = open("{}_{}_sel_log_a.csv".format(REPEATTIME, STEP), "w")
	
	st = now()
	lst = st

	print (STEP, 'Task Async Test Start')
	for sti in range(0, REPEATTIME, STEP):
		lp = asyncio.get_event_loop()
		tasks = [asyncio.ensure_future(ins_sql(lp)) for si in range(STEP)]
		# print (tasks)
		lp.run_until_complete(asyncio.wait(tasks))
		# count += len(dones)
		# count_sum = sum(COUNT)
		# if (sti + STEP) % STEP == 0:
			# print ("{} Queries Finished, {} Lines Fetched. Time Elapsed: {:.4}".format(sti+STEP, count_sum, now() - st))
			# logfile.write("{},{},{:.4}\n".format(sti+STEP, count_sum, now()-lst))
			# print ("{} Queries Finished. Time Elapsed: {:.4}".format(sti+STEP, now() - st))
			# logfile.write("{},{:.4}\n".format(sti+STEP, now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print (STEP, 'Tast Async Test Finished', now() - st)
	# logfile.close()


# if SYNC:
# 	selectDbSync()
# else:
# 	selectDbAsync()
# countDb()
# dropDb()

# for STEP in range (8, 18, 2):
# 	COUNT = [0 for i in range(STEP)]
# 	#selectDbSync()
# 	selectDbAsync()
selectDbAsync()