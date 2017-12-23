import json

f = open('reviews_Video_Games_5_strict.json', 'r')

content = f.read()
content = ',\n'.join(content.split('\n'))
content = '[' + content + ']'

q = open('reviews_Video_Games_5_array.json', 'w')
q.write(content)