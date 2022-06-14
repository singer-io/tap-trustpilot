import json

import singer
from tap_trustpilot import transform

LOGGER = singer.get_logger()
PAGE_SIZE = 100
CONSUMER_CHUNK_SIZE = 1000

class IDS(object):
    BUSINESS_UNITS = "business_units"
    REVIEWS = "reviews"
    CONSUMERS = "consumers"

stream_ids = [getattr(IDS, x) for x in dir(IDS)
              if not x.startswith("__")]

PK_FIELDS = {
    IDS.BUSINESS_UNITS: ["id"],
    IDS.REVIEWS: ["business_unit_id", "id"],
    IDS.CONSUMERS: ["id"],
}

class Stream(object):
    def __init__(self, tap_stream_id, path,
                 returns_collection=True,
                 collection_key=None,
                 custom_formatter=None):
        self.tap_stream_id = tap_stream_id
        self.path = path
        self.returns_collection = returns_collection
        self.collection_key = collection_key
        self.custom_formatter = custom_formatter or (lambda x: x)

    def metrics(self, records):
        with singer.metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def format_response(self, response):
        if self.returns_collection:
            if self.collection_key:
                records = (response or {}).get(self.collection_key, [])
            else:
                records = response or []
        else:
            records = [] if not response else [response]

        return self.custom_formatter(records)

    def transform(self, ctx, records):
        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()
        transformed = [transform.transform(item, schema) for item in records]
        return transformed


class BusinessUnits(Stream):

    # tap_stream_id = "business_units"
    stream_name = "business_units"
    key_properties = ['id']
    replication_keys  = None
    replication_method = "FULL_TABLE"
    params = {}

    def raw_fetch(self, ctx, business_unit_id):
        # parameterizing business unit ID to support multiple business unit IDs TDL-19427
        return ctx.client.GET({"path": self.path, "business_unit_id": business_unit_id}, self.tap_stream_id)

    def sync(self, ctx):
        """
        Extracts business unit profile for a given business_unit_id
        Logs a warning message if business_unit_id is invalid
        """
        business_unit_id = ctx.config['business_unit_id']

        resp = self.raw_fetch(ctx, business_unit_id)
        if resp:
            resp['id'] = business_unit_id

            business_units = self.transform(ctx, [resp])

            ctx.cache["business_unit"] = business_units[0]

            self.write_records([ctx.cache["business_unit"]])
        else:
            LOGGER.warning("Business Unit {} was not found!".format(business_unit_id))


class Paginated(Stream):
    @staticmethod
    def get_params(page):
        return {
            "page": page,
            "perPage": PAGE_SIZE,
            "orderBy": "createdat.desc"
        }

    def _sync(self, ctx):
        """
        Extracts data in paginated streams
        default page size is 100 and cannot exceed more than 100 due to trust pilot API restrictions
        """
        business_unit_id = ctx.config['business_unit_id']

        page = 1
        while True:
            LOGGER.info("Fetching page {} for {}".format(page, self.tap_stream_id))
            params = self.get_params(page)
            resp = ctx.client.GET({"path": self.path, "params": params, "business_unit_id":business_unit_id}, self.tap_stream_id)
            raw_records = self.format_response(resp)

            for raw_record in raw_records:
                raw_record['business_unit_id'] = business_unit_id

            records = self.transform(ctx, raw_records)
            self.write_records(records)
            yield records

            if len(records) < PAGE_SIZE:
                break

            page += 1


class Reviews(Paginated):

    key_properties = ["business_unit_id", "id"]
    stream_name = "reviews"
    replication_keys = None
    replication_method = "FULL_TABLE"
    params = {}

    @staticmethod
    def add_consumers_to_cache(ctx, batch):
        for record in batch:
            consumer_id = record.get('consumer', {}).get('id')
            if consumer_id is not None:
                ctx.cache['consumer_ids'].add(consumer_id)

    def sync(self, ctx):
        """
        Extracts data for consumer reviews in paginated manner
        Caches consumer id for each review
        """
        ctx.cache['consumer_ids'] = set()
        for batch in self._sync(ctx):
            self.add_consumers_to_cache(ctx, batch)


class Consumers(Stream):

    key_properties = ['id']
    stream_name = "consumers"
    replication_keys = None
    replication_method = "FULL_TABLE"
    params = {}

    def sync(self, ctx):
        """
        extracts consumer profiles using bulk endpoint.
        Chunks the Consumer IDs set to smaller list of size 1000
        """
        business_unit_id = ctx.config['business_unit_id']

        total = len(ctx.cache.get('consumer_ids',[]))
        LOGGER.info("Total Consumer profiles to be extracted {}".format(total))

        # chunk list of consumer IDs to smaller lists of size 1000
        consumer_ids = list(ctx.cache.get('consumer_ids',[]))
        chunked_consumer_ids = [consumer_ids[i: i+CONSUMER_CHUNK_SIZE] for i in range(0, len(consumer_ids),
                                                                                      CONSUMER_CHUNK_SIZE)]

        for idx, consumer_id_list in enumerate(chunked_consumer_ids):
            LOGGER.info("Fetching consumer profiles page {} of {}".format(idx + 1, len(chunked_consumer_ids)))
            resp = ctx.client.POST({"path": self.path, "payload": json.dumps({"consumerIds": consumer_id_list})},
                                   self.tap_stream_id)

            raw_records = self.format_response([resp])
            raw_records = list(raw_records[0].get('consumers', {}).values())
            for raw_record in raw_records:
                raw_record['business_unit_id'] = business_unit_id

            records = self.transform(ctx, raw_records)
            self.write_records(records)


business_units = BusinessUnits(IDS.BUSINESS_UNITS, "/business-units/:business_unit_id/profileinfo")
all_streams = [
    business_units,
    Reviews(IDS.REVIEWS, '/business-units/:business_unit_id/reviews', collection_key='reviews'),
    Consumers(IDS.CONSUMERS, '/consumers/profile/bulk')
]
all_stream_ids = [s.tap_stream_id for s in all_streams] 
