# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 13:50:00 2018

@author: fangyucheng
"""

import requests


def danmu(x,jyid,targetid):
    danmu_lst=[]
    pagenum_lst=[]
    for i in range(0,x):
        ttt=15*(1+i)
        pagenum_lst.append(ttt)
    for pagenum in pagenum_lst:
        try:
            url='https://mfm.video.qq.com/danmu?otype=json&callback=jQuery'+jyid+'&timestamp='+str(pagenum)+'&target_id='+targetid+'&count=500&second_count=6&session_key=0%2C0%2C0'
            get_page=requests.get(url)
            get_page.encoding='utf-8'
            page=get_page.text
            length=len(jyid)+7
            prepage=page[length:-1]
            prepage=prepage.replace('\r','')
            prepage=prepage.replace('/n','')
            dicdicdic=eval(prepage)
            danmu_count=dicdicdic['count']
            if danmu_count>1000:
                print(danmu_count)
            print(pagenum)
            print('get one page')
            get_danmu_lst=dicdicdic['comments']
            for danmu in get_danmu_lst:
                commentid=danmu['commentid']
                content=danmu['content']
                timepoint=danmu['timepoint']
                upcount=danmu['upcount']
                opername=danmu['opername']
                D0={'commentid':commentid,'content':content,'timepoint':timepoint,'upcount':upcount,'opername':opername}
                danmu_lst.append(D0)
        except SyntaxError:
            print(str(pagenum)+'there is sth wrong')
    return danmu_lst

if __name__=='__main__':
    x=int((1*3600+31*60+28)/15)
    jyid='19103025125001255282_1522399545358'
    targetid='2434347230'
    video_data777=danmu(x,jyid,targetid)
#one    
#1 33 13
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery19104770781474841782_1522044823510&timestamp=15&target_id=2431410170&count=80&second_count=6&session_key=0%2C0%2C0&_=1522044823541

#two
#1 31 28
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery1910006379066561103097_1522048580301&timestamp=15&target_id=2432862868&count=80&second_count=6&session_key=0%2C0%2C0&_=1522048580319

#three
#1 31 28            
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery19104977942731832877_1522048936005&timestamp=45&target_id=2434347230&count=80&second_count=6&session_key=178328%2C326%2C1522048940&_=1522048936017           

#four
#1 31 12
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery191007914957214696439_1522048988303&timestamp=15&target_id=2464055709&count=80&second_count=6&session_key=0%2C0%2C0&_=1522048988318

#five
#1 34 47
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery19109570751887462264_1522049153524&timestamp=45&target_id=2479936974&count=80&second_count=6&session_key=95212%2C150%2C1522049156&_=1522049153539

#six
#1 31 27
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery1910025632186610303198_1522050281547&timestamp=135&target_id=2497027899&count=80&second_count=6&session_key=135654%2C180%2C1522050285&_=1522050281568

#seven
#1 31 22
#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery191022889623011170301_1522050238126&timestamp=15&target_id=2515637880&count=80&second_count=6&session_key=0%2C0%2C0&_=1522050238141


#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery19105157512767429676_1522391911686&timestamp=15&target_id=2515637880&count=80&second_count=6&session_key=0%2C0%2C0&_=1522391911700

#https://mfm.video.qq.com/danmu?otype=json&callback=jQuery19103025125001255282_1522399545358&timestamp=105&target_id=2434347230&count=80&second_count=6&session_key=186386%2C332%2C1522399582&_=1522399545371
