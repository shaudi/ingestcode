import boto3
import gzip
import logging
import requests
import psycopg2
import sys
import tempfile
import ujson

from collections import namedtuple
from contextlib import closing
from datetime import datetime, timedelta, timezone
from requests.auth import HTTPBasicAuth
from time import sleep

def flatten_comments(link_doc):
    # This function takes the link document as produced in
    # do_search_nibble and transforms the comment tree into
    # a flat list that is more amenable to querying with 
    # athena, jq, etc
    # Specifically:
    # - removes the "Listing" level of the "comments" and "replies" properties
    # - removes the link itself from the comments listing (kind="t3")
    # - traverses the "replies" tree and puts each document into the top level 
    # - "flattened_comments" array
    comments = link_doc["comments"]
    flattened_comments = []
    
    # Expect the first element of comments to be "Listing" type with length 1,
    # and the type of the first element in the Listing to be "t3" (link)
    assert comments[0]["kind"] == "Listing" and \
        len(comments[0]["data"]["children"]) == 1 and \
        comments[0]["data"]["children"][0]["kind"] == "t3"
    
    def traverse(comment_listing):
        assert comment_listing["kind"] == "Listing"
        for child in comment_listing["data"]["children"]:
            clone = dict(child)
            clone["data"] = {k:v for (k,v) in clone["data"].items() if k != "replies"}
            flattened_comments.append(clone)
            if child["data"].get("replies"):
                traverse(child["data"]["replies"])
    
    for cl in comments[1:]:
        traverse(cl)
    
    return dict(link_doc,flattened_comments=flattened_comments)

class Ringest(object):
    ChildPaths = namedtuple('ChildPaths', ['link_name', 'child_ids'])
    bad_ids = (None, '', '_', '__')

    @property
    def default_rate_limit_remaining(self):
        return 60

    @property
    def default_rate_limit_reset(self):
        return 5

    def __init__(self, component_credentials, bucket_name='cortico-data'):
        self.component_credentials = component_credentials
        self.bucket_name = bucket_name
        self.unames = []
        self.tokens = []
        self.request_sleep = 1
        self.rate_limit_used = 0
        self.rate_limit_remaining = self.default_rate_limit_remaining
        self.rate_limit_reset = 0
        self.last_request_time = None
        self.request_count = 0
        self.sleep_every = 3

    def do_ringest(self, start_time: datetime, end_time: datetime,
                   token_count: int=2, request_sleep: int=1,
                   reservation_hours: int=0, reservation_minutes: int=10,
                   limit=100):
        # Fetching data within a time range entails querying three separate
        # endpoints:
        #
        # 1) /search -- to get links posted within the time frame
        # 2) /comments -- to get the first n top-level comments on a link
        # 3) /api/morechildren -- to get the rest of the comments on a link
        st = datetime.now(timezone.utc)
        s3 = boto3.client("s3")
        search_key = 'reddit/links/' +\
                     start_time.strftime('%Y-%m-%d/%H%M.json.gz')
        self.unames = self.lock_creds(count=token_count,
                                      reservation_hours=reservation_hours,
                                      reservation_minutes=reservation_minutes)
        self.request_sleep = request_sleep
        # self.do_search_experiment(start_time=start_time, part_size_seconds=2,
        #                           limit=limit)
        self.do_search_nibble(s3_client=s3, s3_key=search_key,
                              start_time=start_time, end_time=end_time,
                              part_size_seconds=10,
                              limit=limit)
        et = datetime.now(timezone.utc)
        # really should clean up credential reservation to be cleaner
        self.release_creds()
        logging.info('%d calls in %s.' % (self.request_count, str(et - st)))

    @staticmethod
    def base_headers():
        return {
            'User-Agent': 'Linux:ai.cortico.rProbe:v0.1 (by /u/cortico-ai)'
        }

    def release_creds(self, unames=None, conn=None):
        if unames is None:
            unames = self.unames

        if len(unames) > 0:
            logging.info('Releasing auth creds...')

            ustr = '(' + ','.join(["'" + u + "'" for u in unames]) + ')'
            now = datetime.now(tz=timezone.utc)
            q1 = """   UPDATE auth_library
                          SET available = CURRENT_TIMESTAMP
                        WHERE platform = 'reddit' AND uname in %s
                    RETURNING uname""" % ustr
            if conn is None:
                with closing(self.component_credentials.get_db_connection()) as conn, \
                        conn.cursor() as curs:
                    curs.execute(q1)
                    conn.commit()
            else:
                with conn.cursor() as curs:
                    curs.execute(q1)
                    conn.commit()

            logging.info('Done.')

    def lock_creds(self, count: int=2, reservation_hours: int=0,
                   reservation_minutes: int=10, conn=None):
        q = """   UPDATE auth_library
                     SET available = '%s'
                   WHERE platform = 'reddit' AND available < '%s'
               RETURNING uname"""

        def lock_loop(_conn):
            _unames = []

            max_attempts = 2
            attempts = 0
            with conn.cursor() as curs:
                while len(_unames) < count and attempts < max_attempts:
                    attempts += 1
                    sleep(1)
                    logging.info('Reserving auth creds...')
                    # if we have any, it's not enough, so release the ones we got
                    self.release_creds(unames=_unames, conn=_conn)
                    now = datetime.now(tz=timezone.utc)
                    available = now + timedelta(hours=reservation_hours,
                                                minutes=reservation_minutes)
                    curs.execute(q % (available.isoformat(sep=' '),
                                      now.isoformat(sep=' ')))
                    _conn.commit()
                    _unames = [r[0] for r in curs.fetchall()]
                    logging.info('Done.')
                    if len(_unames) > count:
                        self.release_creds(unames=_unames[count:], conn=_conn)
                        _unames = _unames[:count]

                        curs.execute('select * from auth_library')

            if len(_unames) < count:
                raise ConnectionError("Unable to reserve credentials.")

            return _unames

        unames = []
        if conn is None:
            with closing(self.component_credentials.get_db_connection()) as conn:
                unames = lock_loop(_conn=conn)
        else:
            unames = lock_loop(_conn=conn)

        return unames

    def fetch_tokens(self):
        password_map = {
            'Faahes23QZg7Fw': 'SeQC4OK7gpSb4i4k837GMo_ains',
            'laAeq4H1xGpxIQ': 'Vn9bPKzZxcJnEYnKg6AEe52cy7A',
            'EzDWhK39JN7hOg': 'UgQV1Z-PNSWEspqvu6wSuXVBmUs',
            'sOSbsk4n7hP5ew': '7WZOJLhEeVW2tuu81DLuTbDLvKk',
            'AiVR0xcW7FZHDQ': 'q8dY0rknQGe4WoUi7hCwRvL8mRM',
            '1SF9q6doBTM7Xw': 'zOaYHcQF5Y4Y3pM4XY0LjSyDae4',
            'Pp1SYjBTFJPxXg': 'pphvk1Ld3U71jJbbgFZNfoWVlu0',
            'SinbCmDX2Y3dTg': 'EA5H78qxHwjlhdxBjLq0bBFyax8',
            'i6zXuvsjCYcW2w': '6iygaqQzUbln1Cc2mq1y1X-B0mQ',
            'BKpMuC6ZaYYBSQ': 'xLn99WdorXD8OSoWZDU8zbau_1o',
            'fpAqd_Cvu6lGww': 'kQ-f0AETHs7WpMMDb4Vasz09T48',
            'UmYc7VkTTN-GOA': 'ZE4y2FpwtcrY1wHBE98NH8MLiUQ',
            'hYgy3w8D0ExVYA': 'R3-CyxTt1IE_NR9r7tD0ka2pmic',
            'H-dfuCXGJPDibQ': 'ansVAsyLZeUHa42ztZkPavxS89E',
            'CSudDHcTD31xMg': 'C2V0pnm9K5WB3JUaZS6ypBR8prc',
            'GVSHiTvtT2NtYg': 'DRRhNTTNdLuXeUQCE63CUn3eaIQ',

            'jsgT2jZhMoTdpw': 'lZ7N_XICirw-woP5embX4EVXW5g',
            'PrExSJoSJF8Odg': 'YeR4QyU9ze7fgnn6IiSQXBcl84I',
            'c1GfGn-W5mwhMA': 'XKulAY-_l_y8MiaKGGUhak2f01c',
            'kN5xZ8e84C_ZLg': 'fmcC30ebMlCRiNwmO4pZij5oAZE',
            'pRyF07D9qgwkrA': 'FukIx78hSUEwBXcSxbvQpS90-w4',
            'NmQTUgPrEDwVfQ': 'XdO4LiyMIAmhpbnfFYjEOG5YVtQ',
            '4Tl1srQyzWQq9Q': 'LO7Jp13-cZMLl91XD9b21wm4Ry0',
            'DikErd-RK9a6dg': '8inRSMyU53Uoo_p_-9uN3rvE71E',
            'uLVSduDxuUnVlQ': 'LdgBzU-SgXbR3iEe2U2RMdlejAw',
            '8lQHB4s4J3eo4g': 'zReYzMB2wDxli2mlHU9_zxVcvzY',
            '7j0Kf7Stx2wz8g': 'OUAVUU0jm6NOz64fgX3r07n2x9Y',
            'tOg3MYyAiVZ9SQ': 'D57oOXJwweGU2uwRPHSqy1B2sAA',
            'qmih4x-Ds_asag': '9xvb5imgI30I50BGOxYJccxZWxI',
            '6vxCFKJnuNYlkA': 'uOLoHpZFC84YlH52SvUgEryRWOg',
            'fhNxav9h2jBEqg': 'iTNVB0QUWIGRUOSBYzHWv1VBNCs',
            'OMlZ4Dm_MO2kdg': '9wucDKW7X4S7ef1urxxR7OWc6cg',

            '-2BIjqi2Y1R1uw': 'CABnoXVq9zBu4yEvwM1TZbR6PUU',
            'BYHPd1zPGMIqow': '5C2UirejH9hp10d5awYfiBwmccI',
            'ZTDeQZolrFOGkQ': 'gQmnaW__xk7Wd9kis6CJQTi8pu0',
            'FwxYSFBwmksGqw': 'fNmiYjMHEVqxjz_U3yD66xoKrYM',
            '4hpmrTmGS3Fpog': 'yshVS1jhy8z3VYKa4iHxbh_rsGY',
            'OigG49ol-B8iyg': 'ox1XCCTSblqaUDjl7Kd2OnhRu3g',
            'ZDKBeFrJTHYjaw': 'MFpzpMQuK7SYciF6eqVPjHP_EIY',
            'YEKCtuleUbofxw': 'OPCUEMDou-8266OFjfTicYW61CE',
            'fI9TvhYG1aLyWg': '6pzzPxQ7SltfGnp4zuKGxbncvKI',
            '6nM3QgAyT0pFew': 't2TUX7cmkVLTaxwhEAB54bPpFno',
            'd6IVDISzsp0OGA': 'i336d-A3zr6UFHDnjSS0Q6Bcg1A',
            'dOCeG8cp27s3CQ': 'Gbqa5b_jW3yoFalwINbm743ZmR0',
            'JYhur3ostoTExw': '6WR8mBolNoYl3J4v55Y0NggnPdU',
            'FJEh0N3Ue-2uMA': 'KGTNfJDoAyeFnGspfk2dimxkk4w',
            'o9Y0u9aX9eDylw': 'o1bfyaUMdoxXHBTC3J6ba7BvAGQ',
            'WndOVZdQQtr4WQ': 'HcLHk9cCQtCfFQwoRnRkop_WyzY'
        }

        if len(self.tokens) < 1:
            unames = self.unames
            logging.info('Requesting tokens...')
            client_auths = [HTTPBasicAuth(username=u, password=password_map[u])
                            for u in unames]
            # client_auths = [HTTPBasicAuth(username=u, password=p)
            #                 for u, p in password_map.items()]
            post_data = {
                'grant_type': 'client_credentials'
            }
            headers = Ringest.base_headers()
            responses = [
                requests.post(url='https://www.reddit.com/api/v1/access_token',
                              auth=client_auth,
                              headers=headers,
                              data=post_data) for client_auth in client_auths]

            for response in responses:
                if response.status_code != 200:
                    logging.warning('Token request failed: %r' % response)

            self.tokens =\
                [response.json()['access_token'] for response in responses]
            logging.info('Done.')

        return self.tokens

    def request(self, url: str, retries_left: int=5):
        logging.info('Requesting url %s' % url)
        if retries_left == 0:
            logging.error("Retries exhausted, exiting.")
            self.release_creds()
            sys.exit(1)

        if self.rate_limit_remaining == 0:
            logging.warning('Rate limit exceeded, rate limit stats:\n' +
                            (('Ratelimit-Used: %d\n' +
                              'Ratelimit-Remaining: %d\n' +
                              'Ratelimit-Reset: %d\n') %
                             (self.rate_limit_used,
                              self.rate_limit_remaining,
                              self.rate_limit_reset)))
            sleep(self.rate_limit_reset)

        tokens = self.fetch_tokens()

        headers = Ringest.base_headers()
        headers['Authorization'] = \
            'Bearer ' + tokens[self.request_count % len(tokens)]

        # if self.request_count % self.sleep_every == 0:
        if self.request_sleep is not None and self.request_sleep > 0:
            sleep(self.request_sleep)
        self.request_count += 1
        response = requests.get(url, headers=headers)

        self.rate_limit_used = response.headers.get('X-Ratelimit-Used', 0)
        self.rate_limit_remaining =\
            response.headers.get('X-Ratelimit-Remaining',
                                 self.default_rate_limit_remaining)
        self.rate_limit_reset =\
            response.headers.get('X-Ratelimit-Reset',
                                 self.default_rate_limit_reset)

        if response.status_code == 414:
            logging.warning('GOT 414, FIX LONG URIs')
            self.release_creds()
            sys.exit(1)
        elif response.status_code != 200 and retries_left > 0:
            logging.warning(('Got status code %d, will sleep and retry, ' +
                             'response: %r') %
                            (response.status_code, response))
            sleep(10)
            response = self.request(url, retries_left - 1)

        return response

    @staticmethod
    def partition_window(start_time: datetime, end_time: datetime,
                         part_size_seconds: int=5, utc_offset: int=0):
        start_uts = int(start_time.timestamp()) + utc_offset
        end_uts = int(end_time.timestamp()) + utc_offset
        return [(i, min(i + part_size_seconds - 1, end_uts))
                for i in range(start_uts, end_uts, part_size_seconds)]

    # Repeated trials iterating over a time frame in a vain attempt to get
    # all link_ids posted in that time frame consistently.  As of now, the
    # reddit search API simply isn't consistent.
    def do_search_experiment(self, start_time: datetime,
                             part_size_seconds: int=5, limit: int=100):
        utc_offset = 28800
        start_uts = int(start_time.timestamp()) + utc_offset
        end_uts = start_uts + part_size_seconds - 1

        link_ids_map = {}
        n_intervals = 10
        n_trials = 10
        for i in range(n_trials):
            link_ids = []
            for j in range(n_intervals):
                offset = j * part_size_seconds
                url = 'https://oauth.reddit.com/search.json?type=link&' + \
                      'sort=new&t=all&syntax=cloudsearch&' + \
                      'q=%28and+timestamp%3A' + \
                      ('%d..%d%%29&' % (start_uts + offset,
                                        end_uts + offset)) + \
                      ('limit=%d' % limit)
                response = self.request(url=url, retries_left=5)
                response_j = response.json()
                after = response_j.get('data', dict()).get('after', None)

                if after is not None:
                    logging.warning('OVERFLOW')

                link_ids.append(sorted([link_thing['data']['id']
                                        for link_thing in
                                        response_j['data']['children']]))
            link_ids_map[i] = link_ids

        for j in range(n_intervals):
            print('interval %d distinct results = %d' %
                  (j, len(set([str(link_ids_map[i][j]) for i in range(n_trials)]))))

    def do_search_nibble(self, s3_client, s3_key: str,
                         start_time: datetime, end_time: datetime,
                         part_size_seconds: int=5, limit: int=100):
        # Critical reddit timestamp minutiae:
        # 1) The search endpoint date range field is called "timestamp".
        # 2) Reddit "things" (the root of the object model they return) have
        #    "created" and "created_utc".
        # 3) "created" is in "local-epoch" time, by which they mean UTC+8.
        # 4) The search endpoint's "timestamp" field maps to thing's "created"
        #    field.
        (_, path) = tempfile.mkstemp()
        with gzip.open(path, 'wt', encoding='utf8') as f:
            # since paging is broken, try to nibble in bites small enough not
            # to get paged
            # also calling anything is broken.  No matter what time window you
            # specify, you're going to get back at most the last 2.5 minutes or
            # so of links
            for start_uts, end_uts in Ringest.\
                    partition_window(start_time=start_time, end_time=end_time,
                                     part_size_seconds=part_size_seconds,
                                     utc_offset=28800):
                url_base = 'https://oauth.reddit.com/search.json?type=link&' + \
                           'sort=new&t=all&syntax=cloudsearch&' + \
                           'q=%28and+timestamp%3A' + \
                           ('%d..%d%%29&' % (start_uts, end_uts)) + \
                           ('limit=%d' % limit)

                started = False
                after = None
                url = url_base
                while (not started) or after is not None:
                    started = True

                    if after is not None:
                        url = url_base + ('&after=%s' % after)
                        logging.warning('Paging results...')

                    response = None
                    try:
                        response = self.request(url=url, retries_left=20)
                    except Exception as e:
                        logging.warning('Problem fetching url: %s' % url)
                        logging.warning(str(e))
                        self.release_creds()
                        sys.exit(1)

                    response_j = response.json()
                    after = response_j.get('data', dict()).get('after', None)

                    for link_thing in response_j['data']['children']:
                        link_doc = flatten_comments({
                            'link': link_thing,
                            'comments':
                                self.do_comments(link_id=link_thing['data']['id'],
                                                 limit=limit)
                        })
                        f.write('%s\n' % ujson.dumps(link_doc))

        try:
            s3_client.upload_file(Filename=path, Bucket=self.bucket_name,
                                  Key=s3_key)
            logging.info("Finished uploading to s3://%(bucket)s/%(key)s" %
                         {'bucket': self.bucket_name, 'key': s3_key})
        except Exception as e:
            logging.warning('Problem uploading to s3://%(bucket)s/%(key)s: %(ex)r' %
                            {'bucket': self.bucket_name, 'key': s3_key, 'ex': e})
            self.release_creds()

    @staticmethod
    def get_children_from_listing(d: dict) -> list:
        assert(isinstance(d, dict))
        assert(d.get('kind') == 'Listing')
        assert(d.get('data', None) is not None)
        assert(isinstance(d.get('data').get('children'), list))

        return d.get('data').get('children', [])

    def populate_thing(self, d: dict, link_name: str):
        kind = d.get('kind')

        if kind == 'Listing':
            children = d['data'].get('children', [])
            if len(children) > 0:
                d['data']['children'] =\
                    [self.populate_thing(d=c, link_name=link_name)
                     for c in children]
        elif kind == 't1':
            d['replies'] = self.populate_thing(d=d.get('replies', dict()),
                                               link_name=link_name)
        elif kind == 'more':
            d['data']['children'] = self.\
                do_morechildren(link_name=link_name,
                                child_ids=d['data'].get('children', []))
        elif kind == 't3':
            pass
        else:
            pass
            # logging.warning('Populate thing: Unrecognized type: %r' % d)

        return d

    def do_comments(self, link_id: str, limit: int=100) -> list:
        url = 'https://oauth.reddit.com/comments/%s.json?limit=%d' % \
              (link_id, limit)

        # SO WEIRD...
        #
        # For reference:
        # "kind" = "t3" -> link (i.e. the head of a thread)
        # "kind" = "t1" -> comment
        #
        # Paging through comments doesn't work as such.
        # You fetch comments from the /comments endpoint
        # (refer to response structure documented below)
        # and if there are more comments, this is flagged in one of two ways...
        #
        # 1) a t1 doc's "replies" field can have a "more" doc in its
        #    data.children array.
        # 2) the final comment Listing (technically maybe any of them???)
        #    can have a "more" doc in its data.children array
        #
        # Fetch the "more"s using the /api/morechildren endpoint
        #
        # The response from /comments will be a list of Listings of the form:
        # [
        #    { "kind": "Listing",
        #      "data": {
        #        "children": [
        #          { "kind": "t3", ... }
        #        ]
        #      }
        #    },
        #    { "kind": "Listing",
        #      "data": {
        #        "children": [
        #          { "kind": "t1",
        #            "replies": {
        #              "kind": "Listing",
        #              "data": {
        #                "children": [
        #                  { "kind": "t1", ... },
        #                  ...,
        #                  { "kind": "more", ... }
        #                ]
        #              }
        #            }
        #          },
        #          ...,
        #          { "kind": "t1", ... }
        #        ]
        #      }
        #    },
        #    ...,
        #    { "kind": "Listing",
        #      "data": {
        #        "children": [
        #          { "kind": "t1", ... },
        #          ...,
        #          { "kind": "t1", ... },
        #          { "kind": "more",
        #            "data": {
        #              "children": [
        #                child_id_1,
        #                ...,
        #                child_id_n
        #              ]
        #            }
        #          },
        #        ]
        #      }
        #    }
        # ]
        response = self.request(url=url, retries_left=2)
        response_j = response.json()
        link_name = response_j[0].get('data', dict()).get('children', [])[0].\
            get('data', dict()).get('name', None)

        if len(response_j) > 1:
            return [self.populate_thing(d=listing, link_name=link_name)
                    for listing in response_j]

        return response_j

    @staticmethod
    def get_child_ids_from_thing(d: dict) -> list:
        if not isinstance(d, dict):
            # print('get_child_ids_from_thing: not a dict: %r' % d)
            return []

        kind = d['kind']

        if kind == 'Listing':
            results = []
            for c in Ringest.get_children_from_listing(d):
                l = Ringest.get_child_ids_from_thing(c)
                if isinstance(l, list) and len(l) > 0:
                    results.extend(l)
            return results
        elif kind == 't1':
            return Ringest.get_child_ids_from_thing(
                d.get('data', dict()).get('replies', dict()))
        elif kind == 'more':
            return [i for i in d.get('data', dict()).get('children', [])
                    if i not in Ringest.bad_ids]
        else:
            return []

    def do_morechildren(self, link_name: str, child_ids: list, limit: int=100)\
            -> list:
        results = []
        more_child_ids = child_ids
        while len(more_child_ids) > 0:
            url = 'https://oauth.reddit.com/api/morechildren?' + \
                  ('link_id=%s&children=%s&' %
                   (link_name, ','.join(more_child_ids[:10]))) + \
                  'api_type=json&limit=%d&sort=old' % limit

            response = self.request(url=url, retries_left=20)
            response_j = response.json()

            more_child_ids = more_child_ids[10:]
            if isinstance(response_j, dict):
                results.append(response_j)

                for thing in response_j.\
                        get('json', dict()).\
                        get('data', dict()).\
                        get('things', []):
                    l = self.get_child_ids_from_thing(thing)
                    if isinstance(l, list) and len(l) > 0:
                        more_child_ids.extend(l)

        return results
