# -*- coding:utf-8 -*-
# @Time : 2019/9/29 11:53 
# @Author : litao
import subprocess,time

f = open("/home/hanye/crawlersNew/crawler/tasks/log", "w", encoding="utf-8")
while True:
    try:
        cmd0 = r'git pull origin master'
        cmd1 = r'git push gitLab master'
        cmdpath = r'/home/hanye/crawlersNew/v-scope2'
        process = subprocess.Popen(cmd0, shell=True, cwd=cmdpath)
        process.wait()
        result = process.returncode
        print(result,file=f)
        process = subprocess.Popen(cmd1, shell=True, cwd=cmdpath)
        process.wait()
        result = process.returncode
        print(result,file=f)
        #time.sleep(43200)
        time.sleep(70)
    except Exception as e:
        f.write("eroor",e)
        f.flush()

f.close()




