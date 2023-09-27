import glob
import os
import json
import multiprocessing
import threading
import psycopg2
import time
import requests
import mysql.connector

from datetime import datetime
from math import ceil
from pprint import pprint

from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer

processes = []
threads = []

class BackgroundRecording(threading.Thread):
    '''For recording live stream'''
    def __init__(self, audio_url, dest_dir, dest_basename, queue, name='Background Recording'):
        self.audio_url = audio_url
        self.dest_dir = dest_dir
        self.dest_basename = dest_basename
        self.queue = queue
        self.running = False
        # self.c_id = name
        # self.dt = datetime.now()
        super().__init__(name=name)
        self.start()
    
    def start(self):
        self.running = True
        super().start()
        
    def run(self):
        os.makedirs(self.dest_dir, exist_ok=True)
        storing = True
        duration_thresh = 15
        
        while self.running:
            start_time = time.time()
            dt = str(datetime.now()).replace(':', '-')

            sec = dt.split('.')[-2:]
            
            self.dest_filename = os.path.join(self.dest_dir, self.dest_basename+ dt +'.wav')
            try:
                response = requests.get(self.audio_url, stream=True)
            except Exception as e:
                debug_error_log(
                    'Unable to record audio. Check the internet connection.'
                )
                # return
                continue
            if storing:
                with open(self.dest_filename, 'wb') as rec_file:
                    storing = False
                    for idx, chunk in enumerate(response.iter_content(chunk_size=1)):
                        if chunk:
                            rec_file.write(chunk)
                        if idx>=16000*duration_thresh:
                            storing = True
                            # self.count += 1
                            break
                self.queue.put(self.dest_filename)

                duration = time.time() - start_time
                try:
                    time.sleep(duration_thresh-duration-0.11)
                except:
                    pass
                # print(f"duration to rec 1 audio {duration}")
    
    def stop(self):
        self.running = False
        self.join()

def debug_error_log(text:str, timestamp:bool=True):
    with open("error.log", 'a') as err_file:
        text = f"{datetime.now()} {text}" if timestamp else text
        print(text, file=err_file)

def get_channel_id(filepath):
    basename = os.path.basename(filepath)
    basename = os.path.splitext(basename)[0]
    id = basename.split('_')[0]
    return id

def load_config(data_path):
    with open(data_path, 'r') as config_file:
        configs = json.load(config_file)
        # pprint(configs)
        return configs
    
def stop_recoding(data_path):
    configs = load_config(data_path)
    return configs["stop_threads"]

def cores_reqirement(num_sources, num_threads_per_process, max_threads_per_process):
    num_process = ceil(num_sources/num_threads_per_process)
    # validation for maximum core limit
    allow_num_cores = os.cpu_count() - 2    # type:ignore
    if num_process < allow_num_cores:
        return num_process
    else:
        '''
            Need to calculate the number of process after increasing the value of num_threads_per_process.
            This increased value must be such that num_process has value just less than num_cores minus 2.
            And this value should be updated in configs.json file as well.
        '''
        cond = True
        while cond:
            num_threads_per_process += 1
            num_process = ceil(num_sources/num_threads_per_process)
            if num_process < allow_num_cores:
                cond = False
                return num_process
            # number of threads per process limit
            if num_threads_per_process > max_threads_per_process:
                raise ValueError(f"Maximum number of threads per process is 15")

def update_configs(data: dict, is_source=False):
    configs = load_config('configs.json')

    """ # if is_source:
    #     for key, val in data.items():
    #         configs['sources'][key] = val
    # else:
    #     for key, val in data.items():
    #         if configs.get(key) is not None:
    #             configs[key] = val
    #         else:
    #             print(f"Can't add new key-value pairs in parent level.") """
    for key, val in data.items():
        if is_source:
            configs['sources'][key] = val
        else:
            if configs.get(key) is not None:
                configs[key] = val
            else:
                debug_error_log(f"Can't add new key-value pairs in parent level.")

    with open('configs.json', 'w', encoding='utf-8') as config_file:
        json.dump(configs, config_file, ensure_ascii=False, indent=4)

def filter_results(results):
    filtered_results = {}
    for i in range(len(results['results'])):
        fingerprinted_confidence = results['results'][i]['fingerprinted_confidence']
        input_confidence = results['results'][i]['input_confidence']
        offset_seconds = results['results'][i]['offset_seconds']

        if fingerprinted_confidence>=0.03 and input_confidence>0.15 and offset_seconds>=0:
            # print('validated')
            offset_value = 0.668725
            actual_offset_seconds = round(offset_seconds/offset_value, 2)
            song_id = results['results'][0]['song_id']
            song_name = results['results'][0]['song_name']

            filtered_results[i] = {
                "song_id" : song_id,
                "song_name" : song_name,
                "input_confidence" : input_confidence,
                "fingerprinted_confidence" : fingerprinted_confidence,
                "offset_seconds" : actual_offset_seconds
                
            }
        else:
            pass
            # print('not validated')
    return filtered_results

def match_audio(djv, filepath):
    results = djv.recognize(
        FileRecognizer, 
        filepath
    )
    if results['results']:
        results = filter_results(results)
    else:
        results = {}
    return results

def db_connection():
    conn = psycopg2.connect("dbname=dejavu user=postgres password=root")
    return conn

def mysql_db_conn():
    conn = mysql.connector.connect(
        host="192.168.10.60",
        user="audio_advertisement",
        password="12345678",
        database="radio"
    )
    return conn

def execute_query(query:str, values:tuple=(), insert:bool=False, req_response:bool=False, top_n_rows:int=-1):
    conn = None
    cur = None
    data = None
    skip_commit = False
    try:
        # conn = db_connection()
        conn = mysql_db_conn()
        cur = conn.cursor()
        if insert:
            if not values:
                debug_error_log('Values not supplied for query')
                return
            cur.execute(query, values)
        else:
            if values:
                debug_error_log('Does your query requrires values? ')
                debug_error_log('Executing without using values')
            cur.execute(query)
            skip_commit = True
        
        # For MySQL DB commit should be omitted for data retival
        if not skip_commit:
            conn.commit()
        
        # call fucntion to extract response if response required
        if req_response:
            # data = cur.fetchall() if top_n_rows <= 0 else cur.fetchone() if top_n_rows == 1 else cur.fetchmany(size=top_n_rows)
            data = cur.fetchall()
            
    # except (Exception, psycopg2.DatabaseError) as error:
    except (Exception) as error:
        debug_error_log(f"Error \n{error}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    
    if req_response:
        return data
    return

def create_table():
    query_create_table_advertisements_log = """
        CREATE TABLE IF NOT EXISTS advertisements_log(
            sn SERIAL PRIMARY KEY,
            advert_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            log_time TIMESTAMP NOT NULL
        )
    """
    execute_query(query_create_table_advertisements_log)

def check_duration(advert_id, channel_id):
    # For Postgres query remove `LIMIT 1` and 
    # uncomment subpart in following execute_query()
    query_select = f"""
        SELECT log_time from advertisements_log
        WHERE advert_id={advert_id} AND channel_id='{channel_id}'
        ORDER BY log_time DESC LIMIT 1
    """
    data = execute_query(query_select, req_response=True)#, top_n_rows=1)
    print(f"chck duration: {data = }")

    if data is not None:
        for row in data:
            duration = datetime.now() - row[0] if isinstance(row, tuple) else row
            return duration
    return None
            # print(duration.seconds)
            # if duration.seconds < 30:
            #     '''No need to log'''

def log_needed(advert_id, channel_id):
    duration = check_duration(advert_id, channel_id)
    
    debug_error_log(f"{duration = }")
    try:
        debug_error_log(f"{duration.seconds = }")
    except:
        pass

    if duration is not None and duration.seconds < 30:  # duration.total_seconds() returns float precision
        '''No need to log'''
        return False
    return True

def keep_log(advert_id, channel_id):
    log_status = log_needed(advert_id, channel_id)
    debug_error_log(f"{log_status = }")

    if not log_status:
        return
    
    # TODO grab channel id from database
    query_insert_log = """
        INSERT INTO advertisements_log(advert_id, channel_id, log_time)
        VALUES (%s, %s, %s);
    """
    execute_query(query_insert_log, values=(advert_id, channel_id, datetime.now()), insert=True)

def record_audio(key, data, queue):
    # audio_url, dest_dir, prefix
    return BackgroundRecording(
            data['audio_url'],
            data['dest_dir'],
            data['prefix'], 
            queue,
            name=key
    )

def logging_removing(results, filepath):
    # perform database operation
    # print('results ', results)
    if results:
        for result in list(results.values()):
            # print('single result ', result)
            channel_id = get_channel_id(filepath)
            # keep_log(result['song_id'], source)

            keep_log(result['song_id'], channel_id)
    # remove the file after matching
    try:
        os.remove(filepath)
    except:
        debug_error_log('audio file used by another process, unable to remove') 
    
def format_db_configs(data, base_dir):
    sources = {}
    for row in data:
        source = {}
        id = str(row[0])

        source['audio_url'] = row[1]
        source['dest_dir'] = base_dir + "/" + id
        source['prefix'] = id + "_clip_"

        sources[id] = source
    # print(f"{sources = }")
    return sources
    
def load_config_db():
    query_select_channels = """
        SELECT id, links FROM channels
    """
    data = execute_query(query_select_channels, req_response=True)
    # print(f"{data = }")
    return format_db_configs(data, base_dir="./audio_recordings")
 
def process_run(sources, process_num, num_threads_per_process, sources_keys, queue, djv):
    global threads
    # print(process_num)
    keys_idx = process_num*num_threads_per_process
    # threads = []

    sources_keys = sources_keys[keys_idx : keys_idx + num_threads_per_process]

    for thread_offset, key in enumerate(sources_keys):
        # debug_error_log(f"{process_num}-{key}: {sources[key]}")
        data = sources[key]
        thread = record_audio(
            key=f"P{process_num}-C{key}", 
            data=data, 
            queue=queue
        )
        threads.append(thread)
        debug_error_log(f"P{process_num}-C{key}: Running")

    stop_thread = False
    while True:
        # print('Running in Fg')
        stop_thread = stop_recoding('configs.json')
        for thread in threads:
            for _ in range(thread.queue.qsize()):
                filepath = thread.queue.get()
                # perform audio matching
                if not os.path.exists(filepath):
                    debug_error_log(f"No file named {filepath}")
                    # USE BOOLEAN TO SKIP MATCHING 
                try:
                    results = match_audio(djv, filepath)
                except Exception as e:
                    results = None
                finally:
                    logging_removing(results, filepath)

            if stop_thread:
                thread.stop()
                debug_error_log(f"{thread.name}: Stopped")

            # print(f"Running state <{thread.name}> : {'active' if not stop_thread else 'inactive'}")
        
        if stop_thread:
            break
        time.sleep(1.5)
        # debug_error_log('---'*15, timestamp=False)
    
    # if any files are left to match
    # match them by identifying from folder
    residual_audios = glob.glob("./audio_recordings/**/*.wav") 
    for residual_audio_filepath in residual_audios:
        try:
            results = match_audio(djv, residual_audio_filepath)
        except Exception as e:
            results = None
        finally:
            logging_removing(results, residual_audio_filepath)

def main(configs, queue, djv):
    global processes
    update_requires = configs["update"]
    sources = configs['sources']
    num_threads_per_process = configs['num_threads_per_process']
    max_threads_per_process = configs['max_threads_per_process']

    # print(list(configs['sources'].keys()))
    # TODO read DB to create sources dictionary and assin value to `sources` key 
    sources_keys = list(configs['sources'].keys())

    num_processes = cores_reqirement(len(sources), num_threads_per_process, max_threads_per_process)

    # processes = []
    for process_num in range(num_processes):
        process = multiprocessing.Process(
            target=process_run, args=(configs['sources'], process_num, num_threads_per_process, sources_keys, queue, djv)
        )
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

if __name__=="__main__":
    create_table()

    # initialize dejavu
    with open('./configs/dejavu.cnf.SAMPLE') as f:
        config = json.load(f)
    try:
        djv = Dejavu(config)
    except:
        debug_error_log("Can't initiate Dejavu")
        exit()

    queue = multiprocessing.Queue()

    configs_path = 'configs.json'
    if not os.path.exists(configs_path):
        debug_error_log("No Config file in the directory")
        raise Exception(f"No Config file in the directory")
    configs = load_config(configs_path)

    sources = load_config_db()

    configs['sources'] = sources

    main(configs, queue, djv)

    debug_error_log("Background Threads stopped")
    debug_error_log('---'*15, timestamp=False)
    


    # update_configs(
    #     {
    #     "temp_src": {
    #             "audio_url" : "https://radio-broadcast.ekantipur.com/stream",
    #             "dest_dir" : "./audio_recordings",
    #             "prefix" : "Temp_clip_"
    #         }
    #     }, 
    #     is_source=True
    # )
    # update_configs(
    #     {
    #     "update": 1
    #     }, 
    #     is_source=False
    # )