from crawler_sys.framework.func_get_releaser_id import get_releaser_id

def calculate_txxw_video_id(data_dict):
    try:
        releaser_id = get_releaser_id(platform="腾讯新闻", releaserUrl=data_dict["releaserUrl"])
        video_id = data_dict['video_id']
        if releaser_id:
            return video_id + "_" +releaser_id
        else:
            return video_id
    except:
        print('error in :', data_dict)
        return None

