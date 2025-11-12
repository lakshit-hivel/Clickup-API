"""
Main module for ClickUp to Database sync
Orchestrates the entire sync process
"""
import sys
import traceback
from datetime import datetime

from clickup_api import get_clickup_spaces, get_folders, get_lists_from_folder, get_tasks_from_list
from database import insert_boards_to_db, insert_sprints_to_db, insert_issues_to_db
from mappers import map_folder_to_board, map_list_to_sprint, map_task_to_issue


def sync_clickup_data():
    """Main sync function - fetches ClickUp data and saves to database"""
    print("Starting ClickUp Full Sync")

    
    try:
        now = datetime.now()
        
        # Initialize data collectors
        boards_data = []
        sprints_data = []
        issues_data = []
        
        # Fetch spaces
        print("\n[1/4] Fetching Boards (Folders)...")
        spaces = get_clickup_spaces()
        print(f"Found {len(spaces)} spaces")
        
        # Process each space
        for space in spaces:
            space_id = space.get('id')
            space_name = space.get('name')
            print(f"\nProcessing space: {space_name}")
            
            # Fetch folders (boards)
            folders = get_folders(space_id)
            print(f"  Found {len(folders)} folders (boards)")
            
            for folder in folders:
                folder_id = folder.get('id')
                folder_name = folder.get('name')
                
                # Map folder to board
                board_data = map_folder_to_board(folder, space_id, now)
                boards_data.append(board_data)
                
                # Fetch lists (sprints) for this folder
                print(f"    Fetching sprints from folder: {folder_name}")
                lists = get_lists_from_folder(folder_id)
                print(f"      Found {len(lists)} lists (sprints)")
                
                for clickup_list in lists:
                    list_id = clickup_list.get('id')
                    list_name = clickup_list.get('name')
                    
                    # Map list to sprint
                    sprint_data = map_list_to_sprint(clickup_list, folder_id, now)
                    sprints_data.append(sprint_data)
                    
                    # Fetch tasks (issues) for this list
                    print(f"        Fetching issues from sprint: {list_name}")
                    tasks = get_tasks_from_list(list_id)
                    print(f"          Found {len(tasks)} tasks (issues)")
                    
                    for task in tasks:
                        # Map task to issue
                        issue_data = map_task_to_issue(task, folder_id, list_id, space_id, now)
                        issues_data.append(issue_data)
        
        # Summary
        print("\n" + "="*60)
        print("SYNC SUMMARY")
        print("="*60)
        print(f"Total Boards (Folders): {len(boards_data)}")
        print(f"Total Sprints (Lists): {len(sprints_data)}")
        print(f"Total Issues (Tasks): {len(issues_data)}")
        
        # Insert into database
        if boards_data:
            print("\n[2/4] Inserting boards into database...")
            insert_boards_to_db(boards_data)
        
        if sprints_data:
            print("\n[3/4] Inserting sprints into database...")
            insert_sprints_to_db(sprints_data)
        
        if issues_data:
            print("\n[4/4] Inserting issues into database...")
            insert_issues_to_db(issues_data)
        
        print("\n" + "="*60)
        print("✓ SYNC COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Sync failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    sync_clickup_data()

