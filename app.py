from concurrent.futures import thread
import json
import os, time, requests, io, csv, logging
from flask import Flask, request, Response, send_from_directory
from werkzeug.utils import secure_filename
from flask_cors import CORS
from threading import Thread
from requests.packages import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


app = Flask(__name__)
cors = CORS(app, origins=['http://localhost'])


app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # Max 100MB limit
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['PROCESSED_FOLDER'] = 'processed'
app.config['LOGS_FOLDER'] = 'logs/'

app.config['CALLBACK_URL'] = ''
app.config['BASE_URL'] = '/api'
app.config['ALLOWED_EXTENSIONS'] = {'csv', 'jpg'}
app.config['API_AUTH'] = {}
app.config['API_URI'] = 'https://waba.360dialog.io/v1/messages'
app.config['REQUIRED_FIELDS'] = ['namespace', 'lang_code',
                                 'image_url', 'body_1', 'body_2', 'api_key', 'template_name']

global total_threads_running
total_threads_running = 0

class threading_with_return_values(Thread):
    '''Normally threads do not return anything, overriding the default threading mechanism'''

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)

        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        Thread.join(self)
        return self._return


def get_timestamp():
    return int(time.time() * 1000)

def hit_api(payload, api_key):
    headers = {'D360-API-KEY': api_key, 'Accept': '*/*',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36'}
    # req = requests.post(app.config['API_URI'], json=payload, headers=headers, allow_redirects=False, verify=False)
    
    session = requests.Session()
    retry = Retry(connect=4, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    req = session.post(app.config['API_URI'], json=payload, headers=headers, allow_redirects=False, verify=False)
    
    status_code = req.status_code
    req.close()

    if status_code < 300:
        return True
    else:
        return False


def file_is_allowed(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def get_valid_files():
    filenames = ''
    for x in app.config['ALLOWED_EXTENSIONS']:
        filenames += x + ', '

    return filenames


def prepare_message(namespace, lang_code, wa_id, image, body_1, body_2, template_name):
    return {
        "to": wa_id,
        "type": "template",
        "template": {
            "namespace": namespace,
            "language": {
                "policy": "deterministic",
                "code": lang_code
            },
            "name": template_name,
            "components": [{
                    "type": "header",
                    "parameters": [{
                            "type": "image",
                            "image": {
                                "link": image
                            }
                        }
                    ]
                }, {
                    "type": "body",
                    "parameters": [{
                            "type": "text",
                            "text": body_1
                        }, {
                            "type": "text",
                            "text": body_2 
                        }
                    ]
                }
            ]
        }
    }

def process_csv_data(data, message, api_key, logger):
    processed = []
    successful = 0
    failed = 0

    for row in data:
        try:
            wa_id = row[2]
            message['to'] = wa_id
            logger.info(f"Sending message to: {wa_id} at {get_timestamp()}")
            req = hit_api(message, api_key)
            logger.info(f"Sent successfully to: {wa_id} at {get_timestamp()}")
            
            if (req):
                row.append('success')
                processed.append(row)
                successful += 1
            else:
                logger.error(f"Error while sending message to: {wa_id}")
                row.append('failed')
                processed.append(row)
                failed += 1

        except Exception as ex:
            logger.error(f"Error while sending message to: {wa_id} at {get_timestamp()}")
            logger.error(ex)

            row.append('failed')
            processed.append(row)
            failed += 1
    return processed, failed, successful

def start_job(filename, message, api_key):
    max_threads_per_csv = 2

    total = 0
    successful = 0
    failed = 0

    csv_rows = []
    fields = []
    threads = []

    print(f"Job started at: {get_timestamp()}")

    if not os.path.exists(os.path.join(os.getcwd(), 'logs')):
        os.mkdir(os.path.join(os.getcwd(), 'logs'))

    if not os.path.isfile(app.config['UPLOAD_FOLDER'] + filename):
        with open(app.config['UPLOAD_FOLDER'] + filename, 'w') as f:
            f.write('')

    logging.basicConfig(filename=app.config['LOGS_FOLDER'] + filename + ".log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    with io.open(app.config['UPLOAD_FOLDER'] + filename, 'r+', encoding='utf-8') as f:
        csvreader = csv.reader(f)
        fields = next(csvreader)
        fields.append('Status')
        csv_items = list(csvreader)
        total = len(csv_items)

        final = [csv_items[i * max_threads_per_csv:(i + 1) * max_threads_per_csv] for i in range((len(csv_items) + max_threads_per_csv - 1) // max_threads_per_csv )]

        for elem in final:
            threads.append(threading_with_return_values(target=process_csv_data, args=(elem, message, api_key, logger, )))

        for workers in threads:
            workers.start()
        
        for workers in threads:
            array, f, s = workers.join()
            failed += f
            successful += s

            for arr in array:
                csv_rows.append(arr)


    with io.open(app.config['PROCESSED_FOLDER'] + '/processed-' + filename, 'w', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(csv_rows)

    print(app.config['PROCESSED_FOLDER'] + '/processed-' + filename)
    logging.shutdown()
    
    if os.path.isfile(app.config['UPLOAD_FOLDER'] + filename):
        print('Clearing up the source file...')
        os.remove(app.config['UPLOAD_FOLDER'] + filename)

    print(f"Filename: {filename}, \n Total records: {total}, Successful: {successful}, Failed: {failed}")

    print(f"Job ended at: {get_timestamp()}")
    
        

# Routings

@app.route(app.config['BASE_URL'] + '/broadcast', methods=['POST'])
def insert_item():
    global total_threads_running
    missing_fields = []

    try:
        request_data = request.form
    except:
        return Response(json.dumps({
                "status": "bad-request",
                "message": "Missing field(s): " + ', '.join(app.config['REQUIRED_FIELDS'])
            }),
                status=200,
                mimetype='application/json')

    for item in app.config['REQUIRED_FIELDS']:
        if item not in request_data:
            missing_fields.append(item)

    if (missing_fields):
        return Response(json.dumps({
                "status": "bad-request",
                "message": "Missing field(s): " + ', '.join(missing_fields)
            }),
                status=200,
                mimetype='application/json')

    namespace = request_data['namespace']
    lang_code = request_data['lang_code']

    image_url = request_data['image_url']
    body_1 = request_data['body_1']
    body_2 = request_data['body_2']
    api_key = request_data['api_key']
    template_name = request_data['template_name']

    if 'file' not in request.files:
            return Response(json.dumps({
            "status": "bad-request",
            "message": "No file was supplied"
        }),
            status=200,
            mimetype='application/json')

    file = request.files['file']

    if file.filename == '':
        return Response(json.dumps({
            "status": "bad-request",
            "message": "No filename"
        }),
            status=200,
            mimetype='application/json')
            

    message = prepare_message(namespace, lang_code, '', image_url, body_1, body_2, template_name)

    if file and file_is_allowed(file.filename):
            filename = str(get_timestamp()) + '-' + secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

            worker = Thread(target=start_job, args=((filename, message, api_key, )))
            worker.start()
            total_threads_running += 1

            return Response(json.dumps({
                "status": "Ok",
                "message": "Data published and broadcast process was started successfully"
            }),
                status=200,
                mimetype='application/json')
    else:
        return Response(json.dumps({
                "status": "bad-request",
                "message": "Invalid file-type supplied, valid file-types are: " + get_valid_files()
            }),
                status=200,
                mimetype='application/json')


@app.route('/processed/<filename>', methods=['GET'])
def send_file(filename):
    if not filename:
        return f'Cannot {request.method} /{request.path}'
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename)

@app.route('/<path:path>', methods=['GET', 'PUT', 'POST', 'DELETE'])
def not_found(path):
    return f'Cannot {request.method} /{path}'

@app.route('/', methods=['GET', 'PUT', 'POST', 'DELETE'])
def home_not_found():
    return f'Cannot {request.method} /'