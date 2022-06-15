# tap-trustpilot

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [TrustPilot](https://developers.trustpilot.com/)
- Extracts the following resources:
  - [Business Unit Reviews](https://developers.trustpilot.com/business-units-api#get-a-business-unit's-reviews)
  - [Consumers](https://developers.trustpilot.com/consumer-api#get-the-profile-of-the-consumer(with-#reviews-and-weblinks))
- Outputs the schema for each resource

This tap does _not_ support incremental replication!

## Install

Clone this repository, and then install using setup.py. We recommend using a virtualenv:

```bash
    > python3 -m venv VENV_TAP_TRUSTPILOT
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-trustpilot
    > pip install .
```

### Configuration

Create a `config.json` file that looks like this:

```
{
    "api_key": "...",
    "business_unit_ids": "123abc, 345fgh, tyuie456"
}
```

## Discovery Mode

Run the Tap in Discovery Mode. This creates a catalog.json for selecting objects/fields to integrate:
    
```bash
> tap-trustpilot --config config.json --discover > catalog.json
```
   
See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

## Sync Mode

Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

For Sync mode:
```bash
> tap-trustpilot --config config.json --catalog catalog.json > state.json
```

---

Copyright &copy; 2022 Stitch
