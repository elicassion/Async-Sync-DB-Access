import json
import codecs

f = open('reviews_Video_Games_5_strict.json', 'r')

content = f.read()
content = ',\n'.join(content.split('\n')[:-1])
content = '[' + content + ']'

q = codecs.open('reviews_Video_Games_5_array.json', 'w', 'ascii')
q.write(content)