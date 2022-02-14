# Import CSV to PostgreSQL


import datetime
import json
import os
import pandas
# import requests
import requests

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")

BASE_URL = "<URL>"
HEADER = {"Content-Type": "application/json"}


def append_data_str(payload, key, value, info, replace=False):
    if (value and not info.get(key) or replace) and str(value) != "nan":
        payload[key] = value.capitalize()
    return payload


def append_data_int(payload, key, value, info, replace=False):
    if value and (not info.get(key) or replace):
        try:
            payload[key] = int(value)
        except ValueError:
            pass
    return payload


def _get_dado_dict():
    orig = {
        "Chave": "Valor",
    }

    return {
        value: int(key) for key, value in orig.items()
    }


def _make_df_from_csv(csv_path):
    return pandas.read_csv(csv_path)


def _generate_cnpj_from_row(row):
    if row.get("cnpj"):
        cnpj = str(row.get("cnpj")).zfill(14)
        return cnpj

    return None


def _make_info_dict(response, row):

    try:
        info = json.loads(response.text)
    except Exception as e:
        print(f"{str(row)} -- {e}")
        info = {}

    return info


def _make_payload(cnpj, data, row):
    dict_payload = {
        "cnpj": str(cnpj),
        "<data>": data.get(row['<data>'])
    }

    return json.dumps(dict_payload)


def _make_request_and_get_status(payload):
    response = requests.put(
        BASE_URL,
        data=payload,
        headers=HEADER
    )

    status_response = response.status_code
    status_reason = response.reason
    print(status_response, status_reason)


def import_csv_to_postgres():
    data = _get_dado_dict()
    print(data)

    df = _make_df_from_csv("<csv_file.csv>")
    print(df)

    for index, row in df.iterrows():
        cnpj = _generate_cnpj_from_row(row)

        if not cnpj:
            continue

        # Dados registrados não serão sobrepostos
        url = BASE_URL

        # response = requests.get(url)

        # info = _make_info_dict(response, row)

        payload = _make_payload(cnpj, data, row)

        _make_request_and_get_status(payload)


import_csv_to_postgres()
