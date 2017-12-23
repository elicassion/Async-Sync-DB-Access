import json
import gzip

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.dumps(eval(l))

f = open("reviews_Video_Games_5_strict.json", 'w')
for l in parse("reviews_Video_Games_5.json.gz"):
  f.write(l + '\n')