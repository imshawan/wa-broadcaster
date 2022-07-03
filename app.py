import os, time, requests, threading, io, csv, logging
from flask import Flask, request, Response, send_from_directory
from werkzeug.utils import secure_filename
import json


app = Flask(__name__)

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


def get_timestamp():
    return int(time.time() * 10000)

def hit_api(payload, api_key):
    headers = {'D360-API-KEY': api_key, 'Accept': '*/*'}
    req = requests.post(app.config['API_URI'], json=payload, headers=headers, allow_redirects=False)
    if req.status_code < 300:
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


def start_job(filename, message, api_key):
    total = 0
    successful = 0
    failed = 0

    csv_rows = []
    fields = []

    print(f"Job started at: {get_timestamp()}")

    if not os.path.isfile(app.config['UPLOAD_FOLDER'] + filename):
        with open(app.config['UPLOAD_FOLDER'] + filename, 'w') as f:
            f.write(' ')

    logging.basicConfig(filename=app.config['LOGS_FOLDER'] + filename + ".log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    with io.open(app.config['UPLOAD_FOLDER'] + filename, 'r+', encoding='utf-8') as f:
        csvreader = csv.reader(f)
        fields = next(csvreader)
        fields.append('Status')

        for row in csvreader:
            total += 1
            try:
                wa_id = row[2]
                message['to'] = wa_id
                logger.info(f"Sending message to: {wa_id}")
                req = hit_api(message, api_key)
                
                if (req):
                    row.append('success')
                    csv_rows.append(row)
                    successful += 1
                else:
                    logger.error(f"Error while sending message to: {wa_id}")
                    row.append('failed')
                    csv_rows.append(row)
                    failed += 1

            except Exception as ex:
                logger.error(f"Error while sending message to: {wa_id}")
                logger.error(ex)

                row.append('failed')
                csv_rows.append(row)
                failed += 1

    with io.open(app.config['PROCESSED_FOLDER'] + '/processed-' + filename, 'w', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(csv_rows)

    print(app.config['PROCESSED_FOLDER'] + '/processed-' + filename)
    logging.shutdown()
    
    if os.path.isfile(app.config['UPLOAD_FOLDER'] + filename):
        os.remove(app.config['UPLOAD_FOLDER'] + filename)

    print(f"Job ended at: {get_timestamp()}")
    
        




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

            worker = threading.Thread(target=start_job, args=((filename, message, api_key, )))
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
    print(filename)
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename)
