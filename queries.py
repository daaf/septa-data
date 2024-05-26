import requests
from google.transit import gtfs_realtime_pb2
from urllib import request


BASE_URL = "https://www3.septa.org"


def get_trainview(params=None, headers=None):
    endpoint = "TrainView"
    return send_get_request(endpoint, params, headers)


def get_next_to_arrive(params=None, headers=None):
    endpoint = "NextToArrive"
    return send_get_request(endpoint, params, headers)


def send_get_request(endpoint, params=None, headers=None):

    url = f'{BASE_URL}/api{endpoint}'

    try:
        # Send a GET request to the TrainView endpoint
        response = requests.get(url, params=params, headers=headers)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        # Parse the JSON response
        train_data = response.json()
        
        # Return the train data
        return train_data
        
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")


def get_vehicle_positions(type):
    if type == "bus" or type == "trolley":
        url_segment = "septa-pa-us"
    elif type == "train":
        url_segment = "septarail-pa-us"
    else:
        raise ValueError("Argument provided to get_vehicle_position must be 'bus', 'trolley', or 'train'")

    url = f'{url_segment}/Vehicle/rtVehiclePosition.pb'

    return open_stream(url)


def open_stream(url):
    url=f'{BASE_URL}/gtfsrt/{url}'
    response = request.urlopen(url)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.read())
    return feed.entity