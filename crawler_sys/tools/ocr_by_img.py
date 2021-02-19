# -*- coding:utf-8 -*-
# @Time : 2020/5/29 10:11 
# @Author : litao

from PIL import Image
import pytesseract,os,re
import cv2
import argparse
import cv2
import os
# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-p", "--preprocess", type=str, default="thresh",
	help="type of preprocessing to be done")
args = vars(ap.parse_args())
class Languages:
    CHS = 'chi_sim'
    ENG = 'eng'

def img_to_str(image_path, lang=Languages.CHS):
    # img = Image.open(image_path)
    # width, height = img.size
    # img.show()
    # mode = img.mode

    # print(img.size)
    # thumb = img.crop((10,42,160,150))
    # img.grab(0,0,250,200)
    # thumb.save("thumb.jpg")
    # image = cv2.imread(image_path)
    # gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # # check to see if we should apply thresholding to preprocess the
    # # image
    # if args["preprocess"] == "thresh":
    #     gray = cv2.threshold(gray, 0, 255,
    #                          cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    # # make a check to see if median blurring should be done to remove
    # # noise
    # elif args["preprocess"] == "blur":
    #     gray = cv2.medianBlur(gray, 3)
    # # write the grayscale image to disk as a temporary file so we can
    # # apply OCR to it
    # filename = "thumb.png"
    # cv2.imwrite(filename, gray)
    # thumb = img.crop((40, 30, 100, 70))
    #img.grab((30, 30, 150, 80))
    # thumb.save("thumb.jpg")
    # ,config="-psm 7 digits"
    img = Image.open(image_path)
    # thumb = img.crop((10,42,160,150))
    # thumb = img.crop((40, 30, 100, 70))
    thumb = img.crop((490, 0, 560, 60))
    thumb.save("thumb.jpg")
    return pytesseract.image_to_string(thumb, lang,config="-psm 7 digits")

def file_path_scan(file_path):
    for filename in os.listdir(file_path):
        path = os.path.join(file_path, filename)
        if not os.path.isfile(path):
            continue
        title = img_to_str(path, lang=Languages.CHS)
        print(title)
        try:
            play_count = re.findall("\d+",title)[0]
            #print(play_count)
        except:
            #print(title)
            play_count= 0
        yield filename,play_count


file_path = r'D:\work_file\word_file_new\litao\num'
for filename,play_count in file_path_scan(file_path):
    time_str = filename.replace(".png","")
    time_str = time_str[0:13] +":"+ time_str[13:15]+":"+ time_str[15:]
    # print(time_str)
    print(time_str,play_count)

# print(img_to_str(r'D:\work_file\word_file_new\litao\screen\2020-04-16 202632.png', lang=Languages.CHS))