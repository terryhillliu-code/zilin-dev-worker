import sqlite3
import re
import os
from pathlib import Path
import sys

# Paths
BRAIN_TASK_PATH = Path("~/zhiwei-bot/.zhiwei/active_brain/task.md").expanduser()
TASKS_DB_PATH = Path("~/zhiwei-dev/tasks.db").expanduser()

def parse_markdown_tasks(md_content):
    """
    Parses task.md and returns a list of tasks.
    """
    tasks = []
    pattern = r"^(\s*-\s*\[)([\s/xX])(\]\s*)(.*)$"
    
    for line in md_content.splitlines():
        match = re.match(pattern, line)
        if match:
            status_char = match.group(2).lower()
            title = match.group(4).strip()
            
            status = 'pending'
            if status_char == '/':
                status = 'running'
            elif status_char in ('x', 'X'):
                status = 'done'
            
            tasks.append({'title': title, 'status': status, 'prefix': match.group(1), 'suffix': match.group(3)})
    return tasks

def sync_to_db(tasks):
    """
    Markdown -> DB
    """
    if not TASKS_DB_PATH.exists():
        return

    conn = sqlite3.connect(TASKS_DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    for task in tasks:
        cursor.execute("SELECT id, status FROM tasks WHERE input = ?", (task['title'],))
        row = cursor.fetchone()
        
        if row:
            # MD -> DB: Only override if MD has NEWER progress (e.g. [x] in MD but not in DB)
            # If DB is already 'awaiting_review', it's essentially 'running' for MD
            if row['status'] != task['status'] and task['status'] != 'pending': 
                print(f"MD -> DB: '{task['title']}' -> {task['status']}")
                cursor.execute("UPDATE tasks SET status = ? WHERE id = ?", (task['status'], row['id']))
        else:
            if task['status'] != 'done':
                print(f"MD -> DB (New): '{task['title']}'")
                cursor.execute("INSERT INTO tasks (input, status, backend) VALUES (?, ?, ?)", (task['title'], task['status'], 'claude'))
    
    conn.commit()
    conn.close()

def sync_from_db():
    """
    DB -> Markdown
    """
    if not TASKS_DB_PATH.exists() or not BRAIN_TASK_PATH.exists():
        return

    conn = sqlite3.connect(TASKS_DB_PATH)
    conn.row_factory = sqlite3.Row
    db_tasks = {row['input']: row['status'] for row in conn.execute("SELECT input, status FROM tasks").fetchall()}
    conn.close()

    with open(BRAIN_TASK_PATH, 'r') as f:
        lines = f.readlines()

    new_lines = []
    updated_count = 0
    pattern = r"^(\s*-\s*\[)([\s/xX])(\]\s*)(.*)$"

    for line in lines:
        match = re.match(pattern, line)
        if match:
            prefix = match.group(1)
            old_char = match.group(2)
            suffix = match.group(3)
            title = match.group(4).strip()

            if title in db_tasks:
                db_status = db_tasks[title]
                new_char = old_char
                if db_status == 'done': new_char = 'x'
                elif db_status in ('running', 'awaiting_review'): new_char = '/'
                elif db_status == 'pending': new_char = ' '

                if new_char != old_char.lower():
                    print(f"DB -> MD: '{title}' -> [{new_char}] ({db_status})")
                    line = f"{prefix}{new_char}{suffix}{match.group(4)}\n"
                    updated_count += 1
        
        new_lines.append(line)

    if updated_count > 0:
        with open(BRAIN_TASK_PATH, 'w') as f:
            f.writelines(new_lines)
        print(f"Updated {updated_count} checkboxes in {BRAIN_TASK_PATH.name}")

def main():
    if not BRAIN_TASK_PATH.exists():
        sys.exit(1)

    with open(BRAIN_TASK_PATH, 'r') as f:
        content = f.read()
    tasks = parse_markdown_tasks(content)
    sync_to_db(tasks)
    sync_from_db()

if __name__ == "__main__":
    main()
