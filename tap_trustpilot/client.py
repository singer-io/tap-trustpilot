import requests
from singer import metrics
import singer
import backoff
import base64

LOGGER = singer.get_logger()

BASE_URL = "https://api.trustpilot.com/v1"
AUTH_URL = "{}/oauth/oauth-business-users-for-applications/accesstoken".format(BASE_URL)


class RateLimitException(Exception):
    pass


class UnauthorisedException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()

        self.access_key = config['access_key']
        self._token = None

    def get_token(self, config):
        creds = "{}:{}".format(config['access_key'], config['client_secret']).encode()
        encoded_creds = base64.b64encode(creds)
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': encoded_creds
        }

        data = {
            'grant_type': 'password',
            'username': config['username'],
            'password': config['password']
        }

        resp = requests.post(url=AUTH_URL, headers=headers, data=data)
        resp.raise_for_status()
        return resp.json()

    @property
    def token(self):
        if not self._token:
            raise RuntimeError("Client is not yet authorized")
        return self._token

    def auth(self, config):
        resp = self.get_token(config)
        token = resp['access_token']
        self._token = token

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent

        request.headers['Authorization'] = 'Bearer {}'.format(self._token)
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
        return self.check_for_http_error(response)

    @staticmethod
    def check_for_http_error(resp_object):
        """
        TrustPilot API has 100k record limit for reviews stream
        below condition should handle Pagination limit exceeded error if page value is more than 1000
        sends an empty response
        """
        if resp_object.status_code == 400 and resp_object.json().get('details') == "Pagination limit exceeded.":
            LOGGER.warning("400 Bad Request, Pagination limit exceeded.")
            return []
        """
        send empty response when given business_unit id is invalid or malformed
        """
        if resp_object.status_code == 400 and (resp_object.json().get('message') ==
                                               "The given business unit id was malformed" or
                                               resp_object.json().get('details') == 'Error: valid unitId is required'):
            return []
        """
        following condition handles unauthorised error..validates apiKey and throws exception if it is not valid
        """
        if resp_object.status_code == 401:
            raise UnauthorisedException("Unauthorised...Invalid ApiKey")
        return resp_object.json()

    def GET(self, request_kwargs, *args, **kwargs):
        req = self.create_get_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)

    def POST(self, request_kwargs, *args, **kwargs):
        req = self.create_post_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)
