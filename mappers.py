import uuid
from datetime import datetime
from config import ORG_ID


def map_folder_to_board(folder, space_id, now):
    """Map ClickUp Folder to Board table schema"""
    folder_id = folder.get('id')
    folder_name = folder.get('name')
    
    return {
        'entity_id': str(folder_id),
        'name': folder_name,
        'display_name': None,
        'board_key': str(folder_id),
        'created_at': now,
        'modifieddate': now,
        'org_id': ORG_ID,
        'account_id': str(space_id),
        'active': not folder.get('archived', False),
        'is_deleted': folder.get('archived', False),
        'is_private': folder.get('hidden', False),
        'uuid': str(uuid.uuid4()),
        'avatar_uri': None,
        'self': None,
        'jira_board_id': None,
        'auto_generated_sprint': False,
        'azure_project_id': None,
        'azure_project_name': None,
        'azure_org_name': None,
    }


def map_list_to_sprint(clickup_list, folder_id, now):
    """Map ClickUp List to Sprint table schema"""
    list_id = clickup_list.get('id')
    list_name = clickup_list.get('name')
    
    # Convert timestamps
    start_date = clickup_list.get('start_date')
    if start_date:
        start_date = datetime.fromtimestamp(int(start_date) / 1000)
    
    end_date = clickup_list.get('due_date')
    if end_date:
        end_date = datetime.fromtimestamp(int(end_date) / 1000)
    
    return {
        'id': str(list_id),
        'created_at': now,
        'is_deleted': clickup_list.get('archived', False),
        'modifieddate': now,
        'board_id': str(folder_id),
        'end_date': end_date,
        'goal': clickup_list.get('content'),
        'name': list_name,
        'sprint_jira_id': str(list_id),
        'start_date': start_date,
        'state': clickup_list.get('status'),
        'org_id': ORG_ID,
    }


def map_task_to_issue(task, folder_id, list_id, space_id, now):
    """Map ClickUp Task to Issue table schema"""
    task_id = task.get('id')
    
    # Convert timestamps
    created_at = task.get('date_created')
    if created_at:
        created_at = datetime.fromtimestamp(int(created_at) / 1000)
    else:
        created_at = now
    
    updated_at = task.get('date_updated')
    if updated_at:
        updated_at = datetime.fromtimestamp(int(updated_at) / 1000)
    else:
        updated_at = now
    
    due_date = task.get('due_date')
    if due_date:
        due_date = datetime.fromtimestamp(int(due_date) / 1000)
    
    resolution_date = task.get('date_closed')
    if resolution_date:
        resolution_date = datetime.fromtimestamp(int(resolution_date) / 1000)
    
    # Get assignee (first one if multiple)
    assignees = task.get('assignees', [])
    assignee_id = str(assignees[0].get('id')) if assignees else None
    
    # Get creator
    creator = task.get('creator', {})
    creator_id = str(creator.get('id')) if creator else None
    
    # Get priority
    priority_obj = task.get('priority')
    priority = priority_obj.get('priority') if priority_obj else None
    
    return {
        'id': str(task_id),
        'created_at': created_at,
        'modifieddate': updated_at,
        'board_id': str(folder_id),
        'priority': priority,
        'resolution_date': resolution_date,
        'time_spent': task.get('time_estimate'),
        'parent_id': task.get('parent'),
        'is_deleted': task.get('archived', False),
        'assignee_id': assignee_id,
        'creator_id': creator_id,
        'due_date': due_date,
        'issue_id': str(task_id),
        'key': task.get('custom_id'),
        'parent_issue_id': task.get('top_level_parent'),
        'project_id': str(space_id),
        'reporter_id': creator_id,
        'status': task.get('status', {}).get('status') if task.get('status') else None,
        'summary': task.get('name'),
        'description': task.get('description'),
        'sprint_id': str(list_id),
        'issue_url': task.get('url'),
        'org_id': ORG_ID,
    }

