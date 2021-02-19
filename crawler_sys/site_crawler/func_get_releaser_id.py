# -*- coding:utf-8 -*-
# @Time : 2019/5/30 11:01 
# @Author : litao
import re, requests


def toutiao(releaserUrl):
    if 'www.toutiao.com' in releaserUrl or 'www.365yg.com' in releaserUrl:
        pattern = 'user/[0-9]+'
        re_find = re.findall(pattern, releaserUrl)
        if re_find != []:
            releaser_id = re_find[0].split('/')[1]
        else:
            pattern = 'to_user_id=[0-9]+'
            re_find = re.findall(pattern, releaserUrl)
            if re_find != []:
                releaser_id = re_find[0].split('=')[1]
            else:
                re_find = re.findall("/m(\d+)", releaserUrl)
                if re_find:
                    return re_find[0]
                else:
                    releaser_id = None
        return releaser_id

    elif 'm.toutiao.com' in releaserUrl:
        pattern = 'profile/[0-9]+'
        re_find = re.findall(pattern, releaserUrl)
        if re_find != []:
            releaser_id = re_find[0].split('/')[1]
            return releaser_id

    elif 'm.365yg.com' in releaserUrl:
        pattern = 'to_user_id=[0-9]+'
        re_find = re.findall(pattern, releaserUrl)
        if re_find != []:
            releaser_id = re_find[0].split('=')[1]
        else:
            releaser_id = None
        return releaser_id


def haokan(releaserUrl,**kwargs):
    if "app_id=" in releaserUrl:
        releaser_id_str = ' '.join(re.findall('app_id=.*', releaserUrl))
        releaser_id = ' '.join(re.findall('\d+', releaser_id_str))
        return releaser_id
    elif "app_id" in releaserUrl:
        try:
            releaser_id_str = re.findall("%22(\d+)%22", releaserUrl)[0]
            if releaser_id_str:
                return releaser_id_str
        except:
            releaser_id_str = re.findall('"(\d+)"', releaserUrl)[0]
            if releaser_id_str:
                return releaser_id_str
    else:
        releaser_id_str = re.findall('(\d+)', releaserUrl)[0]
        if releaser_id_str:
            return releaser_id_str


def tengxunshipin(releaserUrl):
    get_page = requests.get(releaserUrl, timeout=2)
    get_page.encoding = 'utf-8'
    page = get_page.text
    try:
        USER_INFO = re.findall("var USER_INFO = ({.*?})", page, flags=re.DOTALL)[0]
        releaser = re.findall("name: '(.*)',", USER_INFO)[0]
        releaser_id = re.findall("number: '(.*)',", USER_INFO)[0]
    except:
        return None
    D0 = {'releaser': releaser,
        'releaser_id': releaser_id}
    return D0


def new_tudou(releaserUrl):
    if '==' in releaserUrl:
        releaser_id_str = ' '.join(re.findall('i/.*==', releaserUrl))
        releaser_id = releaser_id_str.replace('i/', '')
        return releaser_id
    elif 'videos' in releaserUrl:
        releaser_id_str = ' '.join(re.findall('i/.*/videos', releaserUrl))
        releaser_id = releaser_id_str.split('/')[1]
        return releaser_id
    else:
        if releaserUrl[-1] == "/":
            releaserUrl = releaserUrl[0:-1]
        releaser_id_str = ''.join(re.findall('i/(.*)', releaserUrl))
        releaser_id = releaser_id_str
        return releaser_id

def douyin(releaserUrl):
    try:
        releaser_id = re.findall("user/(\d+)",releaserUrl)[0]
    except:
        print(releaserUrl)
        return None

    return releaser_id


def tencent_news(releaserUrl):
    releaserUrl = str(releaserUrl)
    try:
        if "media/" in releaserUrl:
            res = re.findall(r"media/(\d+)", releaserUrl)
            if res:
                return res[0]
            else:
                pattern = 'media/[0-9]+'
                re_find = re.findall(pattern, releaserUrl)
                if re_find != []:
                    releaser_id = re_find[0].split('/')[1]
                else:
                    releaser_id = False
                return releaser_id
        else:
            res = re.findall(r"chlid=(\d+)", releaserUrl)
            if res:
                return res[0]
    except:
        return False


def miaopai(releaserUrl):
    if 'n.miaopai.com' in releaserUrl:
        releaser_id_str = releaserUrl.split('/')[-1]
        releaser_id = releaser_id_str.replace('.html', '')
        releaser_id = releaser_id_str.replace('.htm', '')
        return releaser_id
    else:
        print("input illegal releaserUrl %s" % releaserUrl)
        return None


def kwai(releaserUrl):
    if "profile" in releaserUrl:
        res = re.findall(r"/profile/(.+)", releaserUrl)
        if res:
            return res[0]
        else:
            return ""
    if "/u/" in releaserUrl:
        res = re.findall(r"/u/(.+)/", releaserUrl)
        if res:
            return res[0]
        else:
            return ""


def wangyi_news(releaserUrl):
    if "/sub/" in releaserUrl:
        res = re.findall(r"/sub/(.+)\.html", releaserUrl)
        if res:
            return res[0]
        else:
            return None
    if "video" in releaserUrl:
        res = re.findall(r"/list/(.+)/video", releaserUrl)
        if res:
            return res[0]
        else:
            return None
    if "all" in releaserUrl:
        res = re.findall(r"/list/(.+)/all", releaserUrl)
        if res:
            return res[0]
        else:
            return None

def weixin(releaserUrl):
    return releaserUrl

plantform_func = {
    "toutiao": toutiao,
    "haokan": haokan,
    "腾讯视频": tengxunshipin,
    "new_tudou": new_tudou,
    "腾讯新闻": tencent_news,
    "miaopai": miaopai,
    "kwai": kwai,
    "网易新闻": wangyi_news,
    "抖音":douyin,
    "weixin":weixin
}


def get_releaser_id(platform=None, releaserUrl=None,is_qq=False):
    if platform and releaserUrl:
        if platform in plantform_func:
            func = plantform_func[platform]
            res = func(releaserUrl)
            if res:
                if not is_qq:
                    if platform == "腾讯视频":
                        return res["releaser_id"]
                    else:
                        return res
                else:
                    return res
            else:
                print(platform, releaserUrl, "can't git releaser_id")
                return None
        else:
            # print(plantform," not in target list")
            return None


if __name__ == "__main__":
    # file = r'D:\work_file\发布者账号\SMG.csv'
    # with open(file, 'r')as f:
    #     head = f.readline()
    #     head_list = head.strip().split(',')
    #     for i in f:
    #         line_list = i.strip().split(',')
    #         line_dict = dict(zip(head_list, line_list))
    #         platform = line_dict['platform']
    #         releaser = line_dict['releaser']
    #         try:
    #             releaserUrl = line_dict['releaserUrl']
    #             if platform == 'new_tudou':
    #                 if releaserUrl[-2:] == '==':
    #                     releaserUrl = releaserUrl + '/videos'
    #                     line_dict['releaserUrl'] = releaserUrl
    #         except:
    #             pass
    #         releaser_id = get_releaser_id(platform=platform, releaserUrl=releaserUrl)
    #         print(platform, releaserUrl, releaser_id)
    releaser_id= get_releaser_id("腾讯新闻","https://r.inews.qq.com/getUserVideoList?chlid=5362294&page_time=&coral_uin=ec8bb1459b9d84100312bf035bb43cd4d0&coral_uid=&type=om&uid=7313ae71df9e5367&omgid=&trueVersion=5.8.00&qimei=287801615436009&devid=008796749793280&appver=23_android_5.8.00&qn-rid=9ec6d3f9-d341-4138-b4e2-6b2ed4b98b5b&qn-sig=891289f9217ec9623723c024dd00eaf5")
    print(releaser_id)