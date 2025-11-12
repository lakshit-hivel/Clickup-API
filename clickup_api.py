import requests
from config import CLICKUP_API_TOKEN, CLICKUP_API_BASE, TEAM_ID


def get_clickup_headers():
    """Return headers for ClickUp API requests"""
    return {
        'Authorization': CLICKUP_API_TOKEN
    }


def get_clickup_spaces():
    """Fetch all spaces from ClickUp team"""
    url = f'{CLICKUP_API_BASE}/team/{TEAM_ID}/space'
    response = requests.get(url, headers=get_clickup_headers())
    response.raise_for_status()
    data = response.json()
    return data.get('spaces', [])


def get_folders(space_id):
    """Fetch all folders in a space"""
    url = f'{CLICKUP_API_BASE}/space/{space_id}/folder'
    response = requests.get(url, headers=get_clickup_headers())
    response.raise_for_status()
    data = response.json()
    return data.get('folders', [])


def get_lists_from_folder(folder_id):
    """Fetch all lists from a folder"""
    url = f'{CLICKUP_API_BASE}/folder/{folder_id}/list'
    response = requests.get(url, headers=get_clickup_headers())
    response.raise_for_status()
    data = response.json()
    return data.get('lists', [])


def get_tasks_from_list(list_id):
    """Fetch all tasks from a list"""
    url = f'{CLICKUP_API_BASE}/list/{list_id}/task'
    response = requests.get(url, headers=get_clickup_headers())
    response.raise_for_status()
    data = response.json()
    return data.get('tasks', [])

