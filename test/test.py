doc = '/projects/KB_recommend/test/depot/content_term.txt_term.txt'
doc_2 = '/projects/KB_recommend/test/depot/content_term.txt_term_2.txt'
data = []
cn = 0
f2 = open(doc_2,mode='w',encoding='utf8')
print('start')
with open(doc,mode='rb') as f:
    for line in f:
        data.append(line)
e=0
w=0
for line in data:
    try:
        #print(line.decode('utf8'))
        f2.write(line.decode('utf8'))
        w=w+1
    except:
        e=e+1
print('end',w,e)
