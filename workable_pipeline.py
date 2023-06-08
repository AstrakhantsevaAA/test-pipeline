import dlt

from pendulum import datetime
from workable import workable_source


#[destination.bigquery.credentials]
creds = {"client_email": "chat-analytics-loader@chat-analytics-rasa-ci.iam.gserviceaccount.com",
"private_key": """-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDz5LZoccgKZ4jH
IFIOz8kn7piZuduuZA4fKZlpMidLhIWGJJ6FjmkD/hC71Vvdm68/e4EohwhE/HDv
CCxZLbM3fwpXOOCogz92dNqhifm+niFT+1ysFIgZ81Vc45ghTtgzY6NO8EqBKsl5
/Rzh+tXC+/reWJAc8qc6qIEsWDD294TjRST9sWkOdPOfCAAjUqoWQ2Z9WehQEwUs
S9mbYzfW05IIUy81y7nNL+q3xSud7/iCIcXxZO8aL9uqUWn3ttQTR65gHclHVYSm
Fwgo4wzZ1fvmjYwsJCljHXAHS8B8PGnMC/t9wGPwCMb0hka1iMDZV/x81yfqRLqp
C6uK8eQXAgMBAAECggEACGRTPm7D0k/Trf5XtHjD1CLqj0eM3ohE0B+vTqZSIdAS
hBYYekH3LRL94mip+4sS0Z1fSVS0HSOOgzsFw5/F7d/qLCwh1sqFUX8d0rcbp8pr
HSel/anLMRFlW4fdQTAkUkhYYyvzdFRTkGX8K25lEot3C1WCx+w1gtgvcaYrMSQr
9GaMjhEScHGIhGQo9OaWpmybdSBGaPm5nIXSC4wM/M9b813/3Mk9CRsbdD05hZDj
MqEpWtqtP90sbZsl5n9N+8vw1eBAgObw0si7B4F7vkm7YnE1AePCF58OSS2En34A
72VcE5TU9hCVC3J4ZusUMGkRq4LzWn/E/nhDHGopqQKBgQD/Bbaw0dV3+coGptmN
FpoCOkZGTvNmIJUIHrEtzFzZqay/BY8VPG/vYPxygoCED9GZnKPsP5a/sE7aL65J
kTRimRLiAn1rT083fOiuPV93S6wlXqw5YP4MULmUg8mNRubbIPNUU6FHQWW9KIO8
hNC3ZcRGcCMswBEKQoYFkRnjTQKBgQD01BOfzwmik3qRefpNPSKDMGwqxPcxU7EC
Ijd/uBTztIxX3eA6zqlZT+q/6rfo+Dfw1OBHW7/ehaUH+ZtZbjS8u7gTVwgzJvlG
8r1NEJQhcdd0putDOsVkUqDx2gmLh669ZT6hYaVZ4lfCKLOoHe6+eBa13sSvR10u
9ElM5Saq8wKBgBgCV7LJ7oj/EVATAURRLmqrRdZ0tGGXC7DaAuBG7y7m1IafZVsg
d4FX5ix5sNO+EYOexagGTJD7blEIUCZI71+g/bAdf+VMcC7PKbDNwmEe1LQn95rn
UMOkDfS3e5A7bpyOu5nizbpBo+xtFgn3jxbVE+d1wzoBUxleLfP0NzW5AoGAL9BH
MNufNxf0RPr8bh81YUeQqF2lJQYCOLdz/UZ3GQ02p2ZWh0Wa/y1DXE27swze5/K7
BlSdyRhynXca5sFGHWHP1j8WA99lqXx4iddmBo4UFN0QbyXILQqSEgmR+aT69FQU
gjHut/ojR3DpfTizSpFrZgNmiBC42xWsRw8tmQ8CgYANzRj6g8gR3zxNbdkxs0lS
pQngo+R3UoX8y32cTtBuufGlFO3WDf8S+tpt2BBqmyGGDkc60TG6nQnAh84xl2lE
p4NB4X84OtV730GMu1ptsMvHr9lgyCtwk3rZzvCEat5IwosWgXnirboVtojOvCkb
UrLFMhD7b+7V4yQNpYqt3A==
-----END PRIVATE KEY-----
""",
"project_id": "chat-analytics-rasa-ci"
 }


def load_all_data() -> None:
    """
    This demo script uses the resources with non-incremental
    loading based on "replace" mode to load all data from provided endpoints,
    and only one "candidate" resource that loads incrementally.
    """
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination='bigquery',
        dataset_name="workable_all_data",
    )
    load_data = workable_source()
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


def load_all_data_with_details() -> None:
    """
    This demo script uses the resources with non-incremental
    loading based on "replace" mode to load all data from provided endpoints,
    and only one "candidate" resource that loads incrementally.

    Additionally, data is loaded from endpoints that depend on the main endpoints.
    Example: /jobs/:shortcode/members, /candidates/:id/comments
    """
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination='bigquery',
        dataset_name="workable_all_data",
        credentials=creds,
    )
    load_data = workable_source(load_details=True)
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


def load_data_by_date() -> None:
    """
    This demo script uses the resources with non-incremental
    loading based on "replace" mode to load all data from provided endpoints,
    and only one "candidate" resource that loads incrementally.

    All non-incremental data filtered by start_date.
    Incremental resource uses start_date as initial_value.
    It does not affect dependent resources (jobs_activities, candidates_activities, etc).
    """
    pipeline = dlt.pipeline(
        pipeline_name="workable",
        destination='bigquery',
        dataset_name="workable_all_data",
    )
    load_data = workable_source(start_date=datetime(2023, 2, 1), load_details=True)
    # run the pipeline with your parameters
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)


if __name__ == "__main__":
    # load_all_data()
    load_all_data_with_details()
    # load_data_by_date()
