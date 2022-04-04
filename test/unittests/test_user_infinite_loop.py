import unittest
from unittest import mock
import tap_zendesk
from tap_zendesk.streams import Users
from tap_zendesk.streams import STREAMS
import singer


class MockSearch:
    def __init__(self):
        self.count = 1001
        self.updated_at = "test"
        
    def __iter__(self):
        return (self for x in range(4))
        
    def search(self, test, updated_after, updated_before, type="user" ):
        # For window size less than 2 return less than or equal to 1000 records and for larger window return greater than 1000 records
        if (singer.strptime(updated_before) - singer.strptime(updated_after)).seconds < 2:
            self.count = 999
        else:
            self.count = 1001
        return self

class TestUserSyncCheck(unittest.TestCase):

    def test_many_records_in_one_seconds_for_user(self):
        """
            Reproduce infinite looping behavior for Users stream when user have many record in single seconds
        """
        user_obj = Users(MockSearch(), {})
        
        with self.assertRaises(Exception) as e:
            l = list(user_obj.sync({'bookmarks': {'users': {'updated_at': '2022-03-30T08:45:21.000000Z'}}}))
            
        self.assertEqual(str(e.exception), 'users - Unable to get all users within minimum window of a single second (2022-03-30T08:45:20Z), found 1001 users within this timestamp. Zendesk can only provide a maximum of 1000 users per request. See: https://develop.zendesk.com/hc/en-us/articles/360022563994--BREAKING-New-Search-API-Result-Limits')
        
    def test_many_records_in_one_seconds_for_user_with_3_sec_window(self):
        """
            To verify that if user give 3 sec window then also we don't get infinite loop behavior
        """
        user_obj = Users(client=MockSearch(), config={'search_window_size': 3})
        
        with self.assertRaises(Exception) as e:
            l = list(user_obj.sync({'bookmarks': {'users': {'updated_at': '2022-03-30T08:45:21.000000Z'}}}))
            
        self.assertEqual(str(e.exception), 'users - Unable to get all users within minimum window of a single second (2022-03-30T08:45:20Z), found 1001 users within this timestamp. Zendesk can only provide a maximum of 1000 users per request. See: https://develop.zendesk.com/hc/en-us/articles/360022563994--BREAKING-New-Search-API-Result-Limits')
        
    