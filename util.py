import json
import logging
import const
import unicodecsv as csv
import sseclient
import requests
import sdk.const as sdkconst

from threep.base import DataYielder

log = logging


def with_urllib3(url):
    """Get a streaming response for the given event feed using urllib3."""
    import urllib3
    urllib3.disable_warnings()
    http = urllib3.PoolManager()
    return http.request('GET', url, preload_content=False)


def with_requests(url):
    """Get a streaming response for the given event feed using requests."""
    return requests.get(url, stream=True)


def get_wikipedia_streams_data():
    url = const.STREAM_URL
    response = with_urllib3(url)  # or with_requests(url)
    parsed = sseclient.SSEClient(response)
    return parsed


class wikipedia_streamsDataYielder(DataYielder):
    def __init__(self, *args, **kwargs):
        self.knowledge = None
        self.batchId = kwargs.get(sdkconst.KEYWORDS.BATCH_ID)
        del kwargs[sdkconst.KEYWORDS.BATCH_ID]
        super(wikipedia_streamsDataYielder, self).__init__(*args, **kwargs)

    def get_format_spec(self):
        """
            :return: format spec as a dictionary in the following format:
                {
                    UNIQUE_COLUMN_IDENTIFIER_1: FORMAT_SPEC for column1,
                    UNIQUE_COLUMN_IDENTIFIER_2: FORMAT_SPEC for column2
                    ...
                }
                FORMAT_SPEC examples:
                 for a DATE type column format could be : '%d-%b-%Y', so, it's entry
                 in the spec would look like:
                        COLUMN_IDENTIFIER: '%d-%b-%Y'

            """
        return {}

    def get_data_as_csv(self, file_path):
        """
            :param file_path: file path where csv results has to be saved
            :return: dict object mentioning csv download status, success/failure
            TODO: return dict format to be standardized
        """
        new_count = 0
        search = self.ds_config[const.CONFIG_FIELDS.SEARCH]
        count = int(self.ds_config[const.CONFIG_FIELDS.COUNT])
        max_count = count + const.MAX_LIMIT
        response = get_wikipedia_streams_data()
        field_names = const.FIELDS_NAMES
        with open(file_path, 'w') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=field_names)
            idx = max_count
            if search != "":
                for event in response.events():
                    change = json.loads(event.data)
                    change = extract_predefined_dict(change)
                    change[sdkconst.KEYWORDS.BATCH_ID] = self.batchId
                    if change[u'title'].lower().find(search.lower()) != -1 and new_count < count:
                        try:
                            writer.writerow(change)
                            new_count += 1
                        except Exception as e:
                            logging.info(e)
                            raise e
                    elif new_count >= count or idx == 0:
                            break
                    idx = idx - 1
        return {}

    def _setup(self):
        """
            one time computations required to pull data from third party service.
            Apart from basic variable initialization done in __init__ method of
            same class, all other datapull readiness logic should be here
       """
        ds_config_key = self.config_key
        identity_key = self.identity_key
        self.identity_config = self.storage_handle.get(sdkconst.NAMESPACES.IDENTITIES,
                                                       identity_key)

        self.ds_config = self.storage_handle.get(identity_key, ds_config_key)

    def reset(self):
        """
            use this method to reset parameters, if needed, before pulling data.
            For e.g., in case, you are using cursors to pull, you may need to reset
            cursor object after sampling rows for metadata computation
            """
        pass

    def describe(self):
        """
            :return: metadata as a list of dictionaries in the following format
                {
                    'internal_name': UNIQUE COLUMN IDENTIFIER,
                    'display_name': COLUMN HEADER,
                    'type': COLUMN DATATYPE -  TEXT/DATE/NUMERIC
               }
        """
        metadata = [
            {
                'internal_name': 'column_0',
                'display_name': 'comment',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_1',
                'display_name': 'wiki',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_2',
                'display_name': 'server_name',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_3',
                'display_name': 'server_script_path',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_4',
                'display_name': 'namespace',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_5',
                'display_name': 'title',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_6',
                'display_name': 'bot',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_7',
                'display_name': 'server_url',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_8',
                'display_name': 'length',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_9',
                'display_name': 'meta',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_10',
                'display_name': 'user',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_11',
                'display_name': 'timestamp',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_12',
                'display_name': 'type',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_13',
                'display_name': 'id',
                'type': sdkconst.DATA_TYPES.NUMERIC
            },
            {
                'internal_name': 'column_14',
                'display_name': 'minor',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_15',
                'display_name': 'revision',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_16',
                'display_name': 'patrolled',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_17',
                'display_name': 'log_id',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_18',
                'display_name': 'log_params',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_19',
                'display_name': 'log_type',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_20',
                'display_name': 'log_action',
                'type': sdkconst.DATA_TYPES.TEXT
            },
            {
                'internal_name': 'column_21',
                'display_name': 'log_action_comment',
                'type': sdkconst.DATA_TYPES.TEXT
            }
        ]

        return metadata


def extract_predefined_dict(data):
    unwanted = set(data) - set(const.FIELDS_NAMES)
    if len(unwanted) != 0:
        for unwanted_key in unwanted:
            if unwanted_key in data:
                del data[unwanted_key]
            else:
                pass
    else:
        pass
    return data
