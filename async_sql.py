import asyncio
import pymysql
import time
from aiomysql import create_pool
st = time.time()
loop = asyncio.get_event_loop()

async def go(i):
	async with create_pool(host='127.0.0.1', port=3306,
						   user='root', password='123456',
						   db='dbtest', loop=loop) as pool:
		async with pool.get() as conn:
			async with conn.cursor() as cur:
				print ("im in", i)
				await cur.execute("SELECT 42;")
				value = await cur.fetchone()
				await asyncio.sleep(1)
				print(value, i)

loop.run_until_complete(asyncio.wait([go(i) for i in range(10)]))
print (time.time() - st)
'''
async def go(db, i):
	print(i)
	print(db)
	with db.cursor() as cur:
		print(cur)
		print ("im in", i)
		await cur.execute("SELECT 42;")
		value = await cur.fetchone()
		await asyncio.sleep(5)
		print(value, i)

loop = asyncio.get_event_loop()
db = aiomysql.connect(host="127.0.0.1", port=3306, user="root",password="123456",db="dbtest",loop=loop )
loop.run_until_complete(asyncio.wait([go(db, i) for i in range(5)]))
#db.close()
'''