__author__ = 'pankaj'

CONFIG_FILE = "wikipedia.json"

FIELDS_NAME = [u'comment', u'wiki', u'server_name', u'server_script_path', u'namespace', u'title', u'bot',
               u'server_url', u'length', u'meta', u'user', u'timestamp', u'type', u'id', u'minor', u'revision',
               u'patrolled', u'log_id', u'log_params', u'log_type', u'log_action', u'log_action_comment']


class IDENTITY_FIELDS:
    pass


class CONFIG_FIELDS:
    SEARCH = "search"
    COUNT = "count"
    COUNT_INCREMENT = 10
