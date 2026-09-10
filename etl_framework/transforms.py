from .sqlite import connect


def run_sql(sql: str, source_db, target_db):
    source = connect(source_db)
    target = connect(target_db)
    try:
        target.executescript(sql)
        target.commit()
    finally:
        source.close(); target.close()
