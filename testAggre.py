import json
import asyncio
import pymysql
import time
import random
from aiomysql import create_pool

now = lambda: time.time()
DATALEN = 231780
REPEATTIME = 40
STEP = 8
SYNC = False

HOST = 'localhost'
USER = 'root'
PORT = 3306
PW = '123456'
DB = 'sjhtest'

COUNT = [0 for i in range(STEP)]

COLUMNS = ['reviewerID', 
			'asin', 
			'helpful',
			'reviewText',
			'overall',
			'summary',
			'unixReviewTime',
			'reviewTime',]

async def exe_sql(lp, i):
	try:
		async with create_pool(host=HOST, port=PORT,
							   user=USER, password=PW,
							   db=DB, loop=lp) as pool:
			async with pool.get() as conn:
				async with conn.cursor() as cur:
					await cur.execute("SELECT count({}) \
										FROM review GROUP BY {};"
										.format(COLUMNS[random.randint(3, 7)], COLUMNS[random.randint(0, 2)]))
					# print (item['reviewerID'])
					# await conn.commit()
					# value = await cur.fetchone()
					# COUNT[i] += value[0]
	except KeyboardInterrupt:
		pass
	except Exception as e:
		print (e)
		# print (item)

def aggreDbSync():
	logfile = open("{}_{}_aggre_log_s.csv".format(REPEATTIME, STEP), "w")
	count = 0

	st = now()
	lst = st
	print (STEP, 'Task Sync Test Start')
	db = pymysql.connect(HOST,USER,PW,DB )
	cursor = db.cursor()
	for i in range(REPEATTIME):
		try:
			cursor.execute("SELECT count({}) \
							FROM review GROUP BY {};"
							.format(COLUMNS[random.randint(0, 7)], COLUMNS[random.randint(0, 7)]))
			# db.commit()
			# value = cursor.fetchone()
			# count += value[0]
		except KeyboardInterrupt:
			pass
		except Exception as e:
			print (e)
		# db.close()
		# if (i+1) % STEP == 0:
		print ("{} Queries Finished, {} Lines Fetched. Time Elapsed: {:.4}".format((i+1), count, now() - st))
		logfile.write("{},{},{:.4}\n".format((i+1), count, now()-lst))
			# print ("{} Queries Finished. Time Elapsed: {:.4}".format((i+1), now() - st))
			# logfile.write("{},{:.4}\n".format((i+1), now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print (STEP, 'Task Sync Test Finished', now() - st)
	logfile.close()

def aggreDbAsync():
	logfile = open("{}_{}_aggre_log_a.csv".format(REPEATTIME, STEP), "w")
	
	st = now()
	lst = st

	print (STEP, 'Task Async Test Start')
	for sti in range(0, REPEATTIME, STEP):
		lp = asyncio.get_event_loop()
		tasks = [asyncio.ensure_future(exe_sql(lp, si)) for si in range(STEP)]
		# print (tasks)
		lp.run_until_complete(asyncio.wait(tasks))
		# count += len(dones)
		count_sum = sum(COUNT)
		if (sti + STEP) % STEP == 0:
			print ("{} Queries Finished, {} Lines Fetched. Time Elapsed: {:.4}".format(sti+STEP, count_sum, now() - st))
			logfile.write("{},{},{:.4}\n".format(sti+STEP, count_sum, now()-lst))
			# print ("{} Queries Finished. Time Elapsed: {:.4}".format(sti+STEP, now() - st))
			# logfile.write("{},{:.4}\n".format(sti+STEP, now()-lst))
		lst = st

	# for item in data[:100]:
	# 	insertData(item)
	print (STEP, 'Tast Async Test Finished', now() - st)
	logfile.close()

aggreDbSync()
aggreDbAsync()

