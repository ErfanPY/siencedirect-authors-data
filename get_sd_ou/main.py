from flask import (Flask, request, render_template, session, flash,
    redirect, url_for, jsonify)

from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField
from wtforms.validators import DataRequired

from celery import Celery

app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_BROKER_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, brocker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

class StartForm(FlaskForm):
    start_year = IntegerField('years', validators=[DataRequired()])
    term = StringField('Search term')
    pub_title = StringField('in jornal or book title ')
    authors = StringField('authors')
    affiliation = StringField('affiliation')
    volume = IntegerField('volume')
    issue = IntegerField('issue')
    page = IntegerField('page')
    keywords = StringField('Title, abstract or author-specified keywords')
    title = StringField('title')
    refrence = StringField('refrences')
    issn = StringField('ISSN or ISBN')
    submit = SubmitField('Start')

def _get_sd_ou_start(**search_kwargs):
    print(**search_kwargs)

@celery.task(bind=True)
def search_task(self, **search_kwargs):
    #TODO this celery decorated function should be inside get_sd_ou and import it here to be called when start clicked on web page
    #
    #
    #    IMPORTANT IMPLIMENTATION IN GET_SD_OU.get_sd_ou for celey self.update
    #  
    #
    #
    log_list = self.info.get('log_list', [])
    self.update_state(state='PROGRESS', meta={'log_list':log_list+log})
    _get_sd_ou_start(**search_kwargs)
    return 

@app.route('/start_search', methods=['POST'])
def search_start():
    
    form = StartForm()
    
    kwargs = {
        'start_year':form.start_year.data, 
        'qs':form.term.data,
        'pub':form.pub_title.data,
        'authors':form.authors.data,
        'affiliations':form.affiliation.data,
        'volume':form.volume.data,
        'issue':form.issue.data,
        'page':form.page.data,
        'tak':form.keywords.data,
        'title':form.title.data,
        'refrences':form.refrence.data,
        'docId':form.issn.data
    }    
    task = search_task.delay(kwargs)
    flash('Start searching in year {}'.format(kwargs['start_year']))
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')
    
@app.route('/status/<task_id>')
def status(task_id):
    task = search_task.AsyncResult(task_id)
    response = {'log_lis':task.info.get('log_list', [])}

    return jsonify(response)
if __name__ == "__main__":
    app.run(debug=True)