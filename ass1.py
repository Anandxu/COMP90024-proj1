"""
Project Name: COMP90024 - HPC Twitter Processing
Author: Guoning QI 1022700 guoningq@student.unimelb.edu.au
        Longxuan XU 963988 longxuanx@student.unimelb.edu.au
Purpose: Identify the top 10 most frequently occurring hashtags(#) and the languages most commonly used for tweeting

Updated on 2020.4.13
"""

from mpi4py import MPI
import json
import sys
import io


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


class Mpi:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.size = self.comm.Get_size()
        self.rank = self.comm.Get_rank()


def traverse(mpi,path):
    hashtags = {}
    languages = {}
    index=0
    file = open(path, 'r', encoding='utf-8')
    for line in file:
            index += 1
            if mpi.rank == index % mpi.size:
                try:
                    f = json.loads(line[:-2])
                except Exception as exception:
                    continue
                if f.get('doc').get('entities').get('hashtags'):
                    hash_list = f.get('doc').get('entities').get('hashtags')
                    hashtag = [each['text'].lower() for each in hash_list]
                    hashtag = list(set(hashtag))
                    for i in hashtag:
                        hashtags[i] = hashtags.get(i, 0) + 1
                if f.get('doc').get('lang'):
                    lang = f.get('doc').get('lang')
                    languages[lang] = languages.get(lang, 0) + 1
    return hashtags, languages


def gather(mpi,dict):  
    dict_list = mpi.comm.gather(dict,root=0)
    if mpi.rank ==0:
        gather_data = {}
        for each in dict_list:
            for k,v in each.items():
                gather_data[k] = gather_data.get(k,0) + v  

        return gather_data

def rank(gather_data):
    L = sorted(gather_data.items(), key = lambda item: item[1], reverse=True)
    L = L[:10]
    return L

def transfer(abbr):
    switcher={
        "en":"English",
        "ar":"Arabic",
        "hy":"Armenian",
        "bn":"Bengali",
        "bg":"Bulgarian",
        "my":"Burmese",
        "cs":"Czech",
        "da":"Danish",
        "de":"German",
        "el":"Greek",
        "es":"Spanish",
        "fa":"Persian",
        "fi":"finnish",
        "fil":"Filipino",
        "fr":"French",
        "he":"Hebrew",
        "hi":"Hindi",
        "hu":"Hungarian",
        "in":"Indonesian",
        "it":"Italian",
        "ja":"Japanese",
        "ko":"Korean",
        "msa":"Malay",
        "nl":"Dutch",
        "no":"Norwegian",
        "pl":"Polish",
        "pt":"Portuguese",
        "ro":"Romanian",
        "ru":"Russian",
        "sv":"Swedish",
        "th":"Thai",
        "tr":"Turkish",
        "uk":"Ukrainian",
        "ur":"Urdu",
        "vi":"Vietnamese",
        "zh-cn":"Chinese Simplified",
        "zh-tw":"Chinese Traditional",
        "tl":"Tagalog",
        "und":"undifined",
        
    }
    return switcher.get(abbr, "Nothing")

def output(top_hashtags,top_languages):
    i=1
    print("Top 10 hashtags:")
    for x in top_hashtags:
        print(str(i)+".#"+x[0]+","+str(x[1]))
        i+=1
    print("")
    print("Top 10 languages:")
    i=1
    for x in top_languages:
        print(str(i)+"."+transfer(x[0])+"("+x[0]+")"+","+str(x[1]))
        i+=1
        
if __name__ == '__main__':
    mpi=Mpi()
    path = "/data/projects/COMP90024/bigTwitter.json"
    hashtags, languages = traverse(mpi,path)
    g_hashtags = gather(mpi,hashtags)
    g_languages = gather(mpi,languages)
    if mpi.rank==0:
        
        top_hashtags=rank(g_hashtags)
        top_languages=rank(g_languages)
        output(top_hashtags,top_languages)


