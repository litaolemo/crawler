# -*- coding:utf-8 -*-
# @Time : 2019/9/26 14:50 
# @Author : litao
import datetime


def week_num(year=None, cycle=None, cycle_num=None, compare_type=None):
    now = datetime.datetime.now()
    now_canlendar = now.isocalendar()
    if not cycle_num:
        week_canlendar = now_canlendar
    else:
        week_canlendar = (now.year, cycle_num + 1, 0)
    year = week_canlendar[0]
    this_week = week_canlendar[1] - 1
    if this_week == 0:
        last_year = year - 1
        this_week = 1
    else:
        last_year = year
    if this_week == 1:
        last_week = "52"
    else:
        last_week = this_week - 1
    today = datetime.datetime(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day)
    # today = datetime.datetime(year=2018, month=12, day=25)
    first_day_in_week = today - datetime.timedelta(
        days=now_canlendar[2] + 7 * (now_canlendar[1] - week_canlendar[1] + 1))
    fisrt_day_ts = int(first_day_in_week.timestamp() * 1e3)
    last_day_in_week = first_day_in_week + datetime.timedelta(days=7)
    last_day_ts = int(last_day_in_week.timestamp() * 1e3)

    this_week_index = 'short-video-weekly'
    this_week_doc = 'daily-url-' + str(year) + '_w' + format(this_week, '>02d') + '_s1'
    last_week_index = 'releaser-weekly-short-video'
    last_week_doc = 'doc'
    if compare_type == "new_released":
        this_week_index = last_week_index
        this_week_doc = last_week_doc
    return this_week_index, this_week_doc, last_week_index, last_week_doc, fisrt_day_ts, last_day_ts, this_week, last_week, last_year

def month_num(year=None, cycle=None, cycle_num=None, compare_type=None):
    now = datetime.datetime.now()
    if not year:
        year = now.year
    if not cycle_num:
        this_mon = now.month - 1 if now.month != 1 else 12
        last_mon = this_mon - 1 if this_mon > 1 else this_mon - 1 + 12
        if this_mon == 12:
            last_year = year - 1
        else:
            last_year = year
    else:
        this_mon = cycle_num
        last_mon = cycle_num - 1 if this_mon > 1 else this_mon - 1 + 12
        if this_mon == 1:
            last_year = year - 1
        else:
            last_year = year
    first_day_ts = int(datetime.datetime(year=last_year, month=this_mon, day=1).timestamp() * 1e3)
    if this_mon == 12:
        next_year = last_year + 1
        next_month = 1
    else:
        next_year = year
        next_month = this_mon + 1
    last_day_ts = int(datetime.datetime(year=next_year, month=next_month, day=1).timestamp() * 1e3)
    this_mon_index = "short-video-production-%s" % last_year
    this_mon_doc = "daily-url-%s" % (
            datetime.datetime(year=last_year, month=next_month, day=1) + datetime.timedelta(days=-1)).strftime(
            "%Y-%m-%d")
    last_mon_index = "releaser"
    last_mon_doc = "releasers"
    if compare_type == "new_released":
        this_mon_index = last_mon_index
        this_mon_doc = last_mon_doc
    return this_mon_index, this_mon_doc, last_mon_index, last_mon_doc, first_day_ts, last_day_ts, this_mon, last_mon, last_year

def quarter_num(year=None, cycle=None, cycle_num=None, compare_type=None):
    now = datetime.datetime.now()
    if not cycle_num:
        this_quarter = int(now.month / 3) + 1
    else:
        this_quarter = cycle_num
    last_quarter = this_quarter - 1 if cycle_num > 1 else 4
    if last_quarter == 4:
        last_year = year - 1
    else:
        last_year = year
    first_day_ts = int(datetime.datetime(year=year, month=(this_quarter - 1) * 3 + 1, day=1).timestamp() * 1e3)
    last_day_ts = int(datetime.datetime(year=year, month=this_quarter * 3 + 1, day=1).timestamp() * 1e3)
    this_quarter_index = "short-video-quarter-%s" % year
    this_quarter_doc = "daily-url-2019-Q%s" % this_quarter
    last_quarter_index = "releaser"
    last_quarter_doc = "releasers-%s-Q%s" % (last_year, last_quarter)
    if compare_type == "new_released":
        this_quarter_index = last_quarter_index
        this_quarter_doc = last_quarter_doc
    return this_quarter_index, this_quarter_doc, last_quarter_index, last_quarter_doc, first_day_ts, last_day_ts, this_quarter, last_quarter, last_year


def func_get_doc_and_timestmp(year=None,cycle="week",cycle_num=None,compare_type=None):
    if cycle == "week":
        this_cycle_index, this_cycle_doc, last_cycle_index, last_cycle_doc, fisrt_day_ts, last_day_ts, this_cycle, last_cycle, last_year = week_num(
            year, cycle, cycle_num, compare_type)
    elif cycle == "month":
        this_cycle_index, this_cycle_doc, last_cycle_index, last_cycle_doc, fisrt_day_ts, last_day_ts, this_cycle, last_cycle, last_year = month_num(
            year, cycle, cycle_num, compare_type)
    elif cycle == "quarter":
        this_cycle_index, this_cycle_doc, last_cycle_index, last_cycle_doc, fisrt_day_ts, last_day_ts, this_cycle, last_cycle, last_year = quarter_num(
            year, cycle, cycle_num, compare_type)
    elif cycle == "year":
        pass
    elif cycle == "all-time":
        return "short-video-all-time-url","all-time-url",None,None
    else:
        return None,None,None,None
    return this_cycle_index,this_cycle_doc,fisrt_day_ts,last_day_ts


if __name__ == "__main__":
    print(func_get_doc_and_timestmp())