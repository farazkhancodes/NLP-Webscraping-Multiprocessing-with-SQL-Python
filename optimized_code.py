from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import pandas as pd
import json,warnings,nltk,os,glob
warnings.filterwarnings("ignore")

from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from  multiprocessing import Process

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

cwd='C:\\Users\\Fuzzy !\\Desktop\\Second Project\\'

def extract_data(daterange,counter):
    for day in range(int(len(daterange)-1)):
        url='https://api.pushshift.io/reddit/submission/search/?after={}&before={}&sort_type=score&subreddit=worldnews&size=10000'.format(daterange[day],daterange[day+1])
        website=session.get(url)
        data=json.loads(website.text)
        data=data['data']
        for subdict in data:
            print('Extracting File No. ',counter.value + 1) 
            url=subdict['full_link']
            time=subdict['created_utc']
            adslash=url.find('r/')
            url=url[:adslash]+'/'+url[adslash:]
            headers = {'User-Agent': 'Mozilla/5.0'}
            response=session.get(url,headers=headers)
            with open(cwd+'\\Soup_Stack\\{}%{}.txt'.format(url[45:-1].replace('/','%'),time),'w',encoding='utf-8') as file:
                file.write(response.text)
            counter.value=counter.value+1
            
    
final_df={'title':[],'url':[],'datetime':[],'score':[],'author':[],'num_comments':[],'pscore':[],'nscore':[]}
final_df=pd.DataFrame(final_df)

with open(cwd+"\\MASTER DICTIONARY\\positive-words.txt") as words:
        pwords=words.read()
        pwords=nltk.word_tokenize(pwords)
with open(cwd+"\\MASTER DICTIONARY\\negative-words.txt") as words:
        nwords=words.read()
        nwords=nltk.word_tokenize(nwords)

def process_data(responses,q,counter):
    data_store={'title':[],'url':[],'datetime':[],'score':[],'author':[],'num_comments':[],'pscore':[],'nscore':[]}
    for response in responses:
        fetch=response.split('%')
        url='https://www.reddit.com/r/worldnews/comments/'+fetch[0]+'/'+fetch[1]+'/'
        time=str(datetime.fromtimestamp(int(fetch[2][:-4])))
        print('Processing File No. ',counter.value)                                                                                                               #DELETE LATER
        with open(response,'r',encoding='utf-8') as file:
            soup=BeautifulSoup(file.read(),'html.parser')
        try:
            title=soup.find('a',{'data-event-action':'title'}).text
        except:
            title='Deleted'
        try:
            score=soup.find('div',{'score unvoted'}).text
        except:
            score=0
        try:
            author_tag=soup.find('p',{'class':'tagline'})
            author=author_tag.find('a').text
        except:
            author='Deleted'
        try:
            num_comments=soup.find('a',{'class':'bylink comments may-blank'}).text[:-9]
        except:
            num_comments=0
        comments=soup.find_all('div',{'md'})
        p,n=0,0
        for comment in comments:
            comment=comment.text
            comment= nltk.word_tokenize(comment)
            for i in comment:
                for j in pwords:
                    if (i==j):
                        p+=1
                for j in nwords:
                    if (i==j):
                        n+=1
        data_store['url'].append(url)
        data_store['datetime'].append(time)
        data_store['title'].append(title)
        data_store['score'].append(score)
        data_store['author'].append(author)
        data_store['num_comments'].append(num_comments)
        data_store['pscore'].append(p)
        data_store['nscore'].append(n)
        counter.value=counter.value-1
    
    df=pd.DataFrame(data_store)
    q.put(df)


def thread_it(daterange,counter):
        parts=np.array_split(daterange,2)
        with ThreadPoolExecutor() as tex:
            tasks=[tex.submit(extract_data,part,counter) for part in parts]


if __name__=='__main__':
    
    dates=pd.date_range(start='06-01-2021',end='12-31-2021')
    os.makedirs('C:\\Users\\Fuzzy !\\Desktop\\Second Project\\Soup_Stack')
    counter=multiprocessing.Manager().Value('i',0)
    parts=np.array_split(dates,multiprocessing.cpu_count())
    processes=[Process(target=thread_it,args=(part,counter)) for part in parts]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    
    q=multiprocessing.Manager().Queue()
    os.chdir('Soup_Stack\\')
    responses = glob.glob('*.txt')
    parts=np.array_split(responses,multiprocessing.cpu_count())
    processes=[Process(target=process_data,args=(part,q,counter)) for part in parts]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    
    while(q.empty()==False):
        df=q.get()
        final_df=pd.concat([final_df,df])

    final_df.datetime=pd.to_datetime(final_df.datetime)
    final_df.sort_values(by=['datetime'],inplace=True)
    final_df.reset_index(inplace=True)
    final_df.drop_duplicates(inplace=True)
    print(final_df)
    os.chdir(cwd)
    final_df.to_excel("Data.xlsx",columns=['url','datetime','title','score','author','num_comments'],sheet_name='Data',index=False)
    final_df.to_excel("Comments.xlsx",columns=['title','pscore','nscore'],sheet_name='Semantic Analysis',index=False)