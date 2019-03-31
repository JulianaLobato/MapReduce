import mincemeat  #Juliana e Flávio
import glob
import csv

text_files = glob.glob('C:\\Users\\julia\\Documents\\Projetos Estudo\\TrabalhoPrático_MapReduce\\Metadados\\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name))for file_name in text_files)

def mapfn(filename, lines):
    print ('map'+ filename)
    from stopwords import allStopWords
    for line in lines.splitlines():
        fields = line.split(':::')
        authors = fields.split('::')[1]
        words = fields[2].lower()
        for author in authors:
            for word in words:
                if (word not in allStopWords):
                    yield (author, word)

def reducefn(author, word):
    print ('reduce' + author)
    import collections
    dict_ = collections.Counter(word)
    #ordena as palavras de acordo com o count
    #result = sorted(dict_.iteritems(), key=lambda item: -item[1])
    return result

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

search = ["Grzegorz Rozenberg", "Philip S. Yu"]

w = csv.writer(open("C:\\Users\\julia\\Documents\\Projetos Estudo\\TrabalhoPrático_MapReduce\\resultado.csv", "w"))

for author in sorted(results.iterkeys()):
    if(filter.count > 0 and author in filter):
        w.write(str(author) + " -> ")
        #apenas as duas palavras que mais aparecem
        for word in results[author][:2]:
            word_count = word[0] + ":" + str(word[1]) + "  "
            w.write(word_count)
        w.write("\n")