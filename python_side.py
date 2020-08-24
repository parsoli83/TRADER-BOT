import emoji
import pandas as pd
import nltk
from random import choice
def search_soli(inp,path):
    def similarity(inp,in2):
        #Bullshit for now but congigurable
        #returns perscent for similarity
        inp=inp.split(" ")
        in2=in2.split(" ")
        total=0
        for i in inp:
            for j in in2:
                if i in j or j in i:
                    total+=1
        if total==0:
            return 10000
        return total*100//(len(inp))//total
    df=pd.read_json(path)
    l_inp=df["input"]
    l_out=df["output"]
    #l_tag=df["tag"]
    best=100000
    best_score=100
    for i in range(len(l_inp)):
        if similarity(inp,l_inp[i])<68 and similarity(inp,l_inp[i])<best_score:
            best=i
            best_score=similarity(inp,l_inp[i])
    if best==100000:
        return False
    else:
        print("👍👍👍")
        print(emoji.emojize('Python is :thumbs_up::thumbs_up::thumbs_up:'))
        print(best_score)
        return choice(l_out[best])








while True:
    print(search_soli(input('Bi: '),r"/home/parsa/Downloads/GIT/elasticbot-master/persian_corpus.json"))