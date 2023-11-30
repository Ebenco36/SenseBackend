from App import db

class Journals(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100))
    description = db.Column(db.Text)

    def __init__(self, title, description):
        self.title = title
        self.description = description

    def __repr__(self):
        return f'<Task {self.title}>'


DBCOllection = db.Column(db.String(100))
citation_type = db.Column(db.String(100))
citation_lang = db.Column(db.String(100))
citation_keywords = db.Column(db.String(100))
journal_title = db.Column(db.String(100))
journal_abstract = db.Column(db.String(100))
journal_source = db.Column(db.String(100))
publication_year = db.Column(db.String(100))
publication_date_obj = db.Column(db.String(100))
domain = db.Column(db.String(100))
other_domain = db.Column(db.String(100))
authors = db.Column(db.String(100))
country = db.Column(db.String(100))
state = db.Column(db.String(100))
PII = db.Column(db.String(100))
journal_URL = db.Column(db.String(100))
document_type = db.Column(db.String(100))
systematic_review_tags = db.Column(db.String(100))
systematic_review_bags = db.Column(db.String(100))
number_of_studies = db.Column(db.String(100))
number_of_studies_bags = db.Column(db.String(100))
population_tags = db.Column(db.String(100))
population_acroymns = db.Column(db.String(100))
population_bags = db.Column(db.String(100))
topic_tags = db.Column(db.String(100))
topic_bags = db.Column(db.String(100))
