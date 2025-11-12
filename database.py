import psycopg2
import traceback
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require',
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise


def insert_boards_to_db(boards_data):
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO insightly_jira.board (
                entity_id, name, display_name, board_key, created_at, modifieddate,
                org_id, account_id, active, is_deleted, is_private, uuid,
                avatar_uri, self, jira_board_id, auto_generated_sprint,
                azure_project_id, azure_project_name, azure_org_name
            ) VALUES (
                %(entity_id)s, %(name)s, %(display_name)s, %(board_key)s, 
                %(created_at)s, %(modifieddate)s, %(org_id)s, %(account_id)s,
                %(active)s, %(is_deleted)s, %(is_private)s, %(uuid)s,
                %(avatar_uri)s, %(self)s, %(jira_board_id)s, %(auto_generated_sprint)s,
                %(azure_project_id)s, %(azure_project_name)s, %(azure_org_name)s
            )
        """
        
        inserted_count = 0
        for board in boards_data:
            try:
                cursor.execute(insert_query, board)
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting board {board.get('name')}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"Successfully inserted/updated {inserted_count} boards")
        
    except Exception as e:
        print(f"Database error: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def insert_sprints_to_db(sprints_data):
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO insightly_jira.sprint (
                id, created_at, is_deleted, modifieddate, board_id,
                end_date, goal, name, sprint_jira_id, start_date,
                state, org_id
            ) VALUES (
                %(id)s, %(created_at)s, %(is_deleted)s, %(modifieddate)s, %(board_id)s,
                %(end_date)s, %(goal)s, %(name)s, %(sprint_jira_id)s, %(start_date)s,
                %(state)s, %(org_id)s
            )
        """
        
        inserted_count = 0
        for sprint in sprints_data:
            try:
                cursor.execute(insert_query, sprint)
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting sprint {sprint.get('name')}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"Successfully inserted/updated {inserted_count} sprints")
        
    except Exception as e:
        print(f"Database error: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def insert_issues_to_db(issues_data):
    
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO insightly_jira.issue (
                id, created_at, modifieddate, board_id, priority,
                resolution_date, time_spent, parent_id, is_deleted,
                assignee_id, creator_id, due_date, issue_id, key,
                parent_issue_id, project_id, reporter_id, status,
                summary, description, sprint_id, issue_url, org_id
            ) VALUES (
                %(id)s, %(created_at)s, %(modifieddate)s, %(board_id)s, %(priority)s,
                %(resolution_date)s, %(time_spent)s, %(parent_id)s, %(is_deleted)s,
                %(assignee_id)s, %(creator_id)s, %(due_date)s, %(issue_id)s, %(key)s,
                %(parent_issue_id)s, %(project_id)s, %(reporter_id)s, %(status)s,
                %(summary)s, %(description)s, %(sprint_id)s, %(issue_url)s, %(org_id)s
            )
        """
        
        inserted_count = 0
        for issue in issues_data:
            try:
                cursor.execute(insert_query, issue)
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting issue {issue.get('summary')}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"Successfully inserted/updated {inserted_count} issues")
        
    except Exception as e:
        print(f"Database error: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

