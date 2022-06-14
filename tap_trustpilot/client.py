import requests
from singer import metrics
import singer
import backoff

LOGGER = singer.get_logger()

BASE_URL = "https://api.trustpilot.com/v1"
AUTH_URL = "{}/oauth/oauth-business-users-for-applications/accesstoken".format(BASE_URL)


class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()

        self.access_key = config['access_key']
        self._token = None


    def auth(self, config):
        '''
            Function auth confirms whether the access_key provided in config.json is valid or not
        '''

        # Using this url to validate the API key
        BU_ALL_URL = "https://api.trustpilot.com/v1/business-units/all"

        headers = {
            'Content-Type': 'application/json',
            'apikey': config["access_key"]
        }

        resp = requests.get(url=BU_ALL_URL, headers=headers)
        if resp.status_code == 200:
            return "Valid API Key"
        else:
            raise RuntimeError("API key is not valid")

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent

        # request.headers['Authorization'] = 'Bearer {}'.format(self._token)
        request.headers['apikey'] = self.access_key
        request.headers['Content-Type'] = 'application/json'

        return self.session.send(request.prepare())

    def url(self, path, business_unit_id=''):
        # parameterizing business unit ID to support multiple business unit IDs TDL-19427
        joined = _join(BASE_URL, path)
        return joined.replace(':business_unit_id', business_unit_id)

    def create_get_request(self, path, business_unit_id, **kwargs):
        return requests.Request(method="GET", url=self.url(path, business_unit_id), **kwargs)

    def create_post_request(self, path, payload, **kwargs):
        return requests.Request(method="POST", url=self.url(path), data=payload, **kwargs)

    @backoff.on_exception(backoff.expo, RateLimitException, max_tries=10, factor=2)
    @backoff.on_exception(backoff.expo, requests.Timeout, max_tries=10, factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 503]:
            raise RateLimitException()
        # below exception should handle Pagination limit exceeded error if page value is more than 1000
        # depends on access level of access_token being used in config.json file
        if response.status_code == 400 and response.json().get('details') == "Pagination limit exceeded.":
            LOGGER.warning("400 Bad Request, Pagination limit exceeded.")
            return []
        response.raise_for_status()
        return response.json()

    def GET(self, request_kwargs, *args, **kwargs):
        req = self.create_get_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)

    def POST(self, request_kwargs, *args, **kwargs):
        req = self.create_post_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)
