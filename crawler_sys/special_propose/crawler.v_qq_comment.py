# -*- coding: utf-8 -*-
"""
Created on Tue Mar 27 15:09:29 2018

@author: fangyucheng
"""

import urllib.request

def get_comment(last):
    comment_lst=[]
    while (type(last)!=bool):
        url = "http://coral.qq.com/article/"+str(targetid)+"/comment/v2?callback=_article"+str(targetid)+"commentv2&oriorder=o&pageflag=1&cursor="+str(last)
        headers = ('User-Agent','Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11')
        opener = urllib.request.build_opener()
        opener.addheaders = [headers]
        data = opener.open(url).read()
        page=str(data,encoding='utf-8')
        useful_infor=page.split('commentv2')[1]
        str_to_dic=useful_infor[1:-1]
        str_to_dic=str_to_dic.replace('true','True')
        str_to_dic=str_to_dic.replace('false','False')
        dic_all_infor=eval(str_to_dic)
        ttt=dic_all_infor['data']
        last=ttt['last']
        print(last)
        repcomment=ttt['repCommList']
        if type(repcomment)==list:
            repcomment_lst=repcomment
            print('repcomment')
        elif type(repcomment)==dict:
            list(repcomment.values())
            print('repcomment')
        else:
            repcomment=None
        if repcomment!=None:
            for yyy in repcomment_lst:
                if type(yyy)==list:
                    for uu in yyy:
                       content=uu['content']
                       parent=uu['parent']
                       publishdate=uu['time']
                       userid=uu['userid']
                       upcount=uu['up']
                       contentid=uu['id']
                       dadorson=2
                       D2={'content':content,'userid':userid,'upcount':upcount,'publishdate':publishdate,'parent':parent,'contentid':contentid,'dadorson':dadorson}
                       comment_lst.append(D2)
                else:
                   content=uu['content']
                   parent=uu['parent']
                   publishdate=uu['time']
                   userid=uu['userid']
                   upcount=uu['up']
                   contentid=uu['id']
                   dadorson=2
                   D2={'content':content,'userid':userid,'upcount':upcount,'publishdate':publishdate,'parent':parent,'contentid':contentid,'dadorson':dadorson}
                   comment_lst.append(D2)
        else:
            print('no repcomment')
        comment=ttt['oriCommList']
        for zzz in comment:
            content=zzz['content']
            contentid=zzz['id']
            upcount=zzz['up']
            publishdate=zzz['time']
            userid=zzz['userid']
            parent=contentid
            dadorson=1
            D0={'content':content,'userid':userid,'upcount':upcount,'publishdate':publishdate,'contentid':contentid,'parent':parent,'dadorson':dadorson}
            comment_lst.append(D0)
    print('get all comment')
    return comment_lst
    
if __name__=='__main__':
    last=0
    targetid=2426229062
    comment=get_comment(last)