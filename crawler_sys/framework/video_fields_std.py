# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 17:57:38 2018

@author: fangyucheng
"""

class Std_fields_video:
    def __init__(self, data_provider=None):
        if data_provider==None:
            data_provider='BDD'
        self.video_data={
                'platform': None,
                'channel': None,
#                'channel_url': None,
#                'channel_subdomain': None,
                'describe': None,
                'title': None,
                'url': None,
                'duration': 0,
                'releaser': None,
                'play_count': None,
                'favorite_count': 0,
                'comment_count': 0,
#                'dislike_count': None,
                'repost_count': None,
                'isOriginal': None,
                'data_provider': data_provider,
                'video_id': None,

                'releaserUrl': None,
                'release_time': 0,
                'fetch_time': 0,
                }

    def field_type_correct(self):
        def none_is_allowed(field, field_type):
            if field not in self.video_data:
                return 'field "%s" is absent' % field
            is_correct=(isinstance(self.video_data[field], field_type)
                    or self.video_data[field]==None)
            if is_correct:
                return True
            else:
                return 'field "%s" should be of [%s] type or None' % (field, field_type.__name__)

        def none_is_not_allowed(field, field_type):
            if field not in self.video_data:
                return 'FATAL: field "%s" is NOT FOUND!' % field
            else:
                is_correct=isinstance(self.video_data[field], field_type)
                if is_correct:
                    return True
                else:
                    return 'field "%s" should be of [%s] type' % (field, field_type.__name__)

        platform_chk=none_is_not_allowed('platform', str)
        duration_chk=none_is_not_allowed('duration', int)
        play_count_chk=none_is_not_allowed('play_count', int)
        favorite_count_chk=none_is_allowed('favorite_count', int)
        comment_count_chk=none_is_allowed('comment_count', int)
#        dislike_count_chk=none_is_allowed('dislike_count', int)
        repost_count_chk=none_is_allowed('repost_count', int)
        isOriginal_chk=none_is_allowed('isOriginal', bool)
        release_time_chk=none_is_allowed('release_time', int)
        fetch_time_chk=none_is_not_allowed('fetch_time', int)

        type_chk={
            'platform': platform_chk,
            'duration': duration_chk,
            'play_count': play_count_chk,
            'favorite_count': favorite_count_chk,
            'comment_count': comment_count_chk,
#            'dislike_count': dislike_count_chk,
            'repost_count': repost_count_chk,
            'isOriginal': isOriginal_chk,
            'release_time': release_time_chk,
            'fetch_time': fetch_time_chk,
            }

        false_time_warning=[]
        for field in type_chk:
            if type_chk[field]!=True:
                false_time_warning.append(type_chk[field])

        if false_time_warning==[]:
            return True
        else:
            warn_msg='WARNING: ' + '\n'.join(false_time_warning)
            return warn_msg



if __name__=='__main__':
    a=Std_fields_video()
    a.video_data['platform']='腾讯视频'
    a.video_data['duration']=62
    a.video_data['title']='This is test video'

#    a.video_data={'duration': 62}
    print(a.field_type_correct())
