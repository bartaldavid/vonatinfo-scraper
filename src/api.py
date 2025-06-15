import requests


def fetch_data():
    response = requests.post(
        "https://vonatinfo.mav-start.hu/map.aspx/getData",
        json={"a": "TRAINS", "jo": {"history": False, "id": False}},
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()
