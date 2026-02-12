def consolidate_docs_master(conn):
    """
    Executes DOCS_MASTER consolidation.
    Uses SQLite-compatible SQL for local execution.
    """
    with open("sql/merge_docs_master_sqlite.sql", "r") as f:
        sql = f.read()

    conn.executescript(sql)
    conn.commit()
