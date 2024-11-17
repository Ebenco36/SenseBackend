from app import db, app

class DatabaseHandler:
    def __init__(self, query):
        self.query = query

    def fetch_papers(self):
        with app.app_context():
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute(self.query)
            papers = cursor.fetchall()
            cursor.close()
            conn.close()
            return papers