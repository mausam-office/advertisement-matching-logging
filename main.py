import glob
import os
import json
import shutil
import threading
import psycopg2
import time
import requests
import mysql.connector
import re

from datetime import datetime, timedelta
from decouple import config
from math import ceil
from multiprocessing import Process, Queue
from pprint import pprint
from requests.exceptions import SSLError

from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer

processes = []
threads = []

# CONFIGS_PATH = 'D:/Anaconda/Audio-FingerPrinting/Desktop-Audio-Matching/configs/configs.json'
# LOG_DIR  = "D:/Anaconda/Audio-FingerPrinting/Desktop-Audio-Matching/errors"
CONFIGS_PATH = 'C:/python-apps/Advertisement-APP/advertisement-matching-logging/configs/configs.json'
LOG_DIR  = "C:/python-apps/Advertisement-APP/advertisement-matching-logging/errors"

class BackgroundRecording(threading.Thread):
    '''For recording live stream'''
    def __init__(self, audio_url:str, dest_dir:str, dest_basename:str, bitrate:int, queue:Queue, name:str='Background Recording'):
        self.audio_url = audio_url
        self.dest_dir = dest_dir
        self.dest_basename = dest_basename
        self.bitrate = bitrate
        self.queue = queue
        self.running = False
        self.delay_min = 5
        self.delay = 5    # SECONDS
        self.iterations = int(16000/127) * self.bitrate
        self.clip_duration = 15    # seconds
        super().__init__(name=name)
        self.reset_retry()
        self.start()
    
    def start(self):
        self.running = True
        super().start()
        
    def run(self):
        os.makedirs(self.dest_dir, exist_ok=True)
        storing = True
        
        
        while self.running:
            start_time = time.time()
            dt = str(datetime.now()).replace(':', '-')

            sec = dt.split('.')[-2:]
            
            self.dest_filename = os.path.join(self.dest_dir, self.dest_basename+ dt +'.wav')
            try:
                response = requests.get(self.audio_url, stream=True)
                content_type = response.headers['Content-Type']
                if content_type not in ['audio/mpeg', 'audio/mp3', 'audio/wav']:
                    try:
                        self.retry_content_error -= 1
                        if self.retry_content_error < 0:
                            debug_error_log(f"{self.name} {content_type = }")
                            debug_error_log(f"{self.name} Response: {str(response.content, 'utf-8')}")
                    except:
                        pass
                    finally:
                        if self.retry_content_error < 0:
                            time.sleep(self.delay*12*self.sleep_factor*5) # _*_*_*n -> n minutes
                            debug_error_log(f"{self.name}: Restarted after content mismatch.")
                            self.sleep_factor += 1
                        continue
            except (Exception, SSLError) as e:
                self.retry_exception -= 1
                time.sleep(self.delay)   # 5 seconds delay for next iteration
                if self.retry_exception < 0:
                    # self.running = False # never stop just delay 
                    debug_error_log(
                        f'{self.getName()}: Unable to record audio due to error\n{str(e)}'
                    )
                    time.sleep(self.delay * 2 * self.sleep_factor)
                    self.sleep_factor += 1
                    debug_error_log(
                        f'{self.getName()}: Re-started'
                    )
                # return
                continue
            self.reset_retry()
            if storing:
                with open(self.dest_filename, 'wb') as rec_file:
                    storing = False
                    for idx, chunk in enumerate(response.iter_content(chunk_size=1)):
                        if chunk:
                            rec_file.write(chunk)
                        if idx>=self.iterations*self.clip_duration:
                            storing = True
                            # self.count += 1
                            break
                self.queue.put(self.dest_filename)

                duration = time.time() - start_time
                try:
                    time.sleep(self.clip_duration-duration-0.11)
                except:
                    pass
                # print(f"duration to rec 1 audio {duration}")
    
    def stop(self):
        self.running = False
        self.join()
        debug_error_log(f"{self.name}: Stopped")

    def reset_retry(self):
        self.retry_exception = 2
        self.retry_content_error = 3
        self.sleep_factor = 1

def debug_error_log(text:str, timestamp:bool=True):
    os.makedirs(LOG_DIR, exist_ok=True)
    # filename -> '/YYYY-MM-DD-error.log'
    filename = '/' + str(datetime.now().date()) + '-' + 'error.log'
    with open(LOG_DIR + filename, 'a') as err_file:
        text = f"{datetime.now()} {text}" if timestamp else text
        print(text, file=err_file)

def get_channel_id(filepath:str):
    basename = os.path.basename(filepath)
    basename = os.path.splitext(basename)[0]
    id = basename.split('_')[0]
    return int(id)

def load_config(data_path:str):
    with open(data_path, 'r') as config_file:
        configs = json.load(config_file)
        # pprint(configs)
        return configs
    
def stop_recoding(data_path:str):
    configs = load_config(data_path)
    return configs["stop_threads"]

def cores_reqirement(num_sources:int, num_threads_per_process:int, max_threads_per_process:int):
    num_cores = os.cpu_count()   # type:ignore
    allowed_num_cores = 1 if num_cores <=2 else 2 \
        if num_cores<=4 else 4 \
            if num_cores<=8 else 8
    debug_error_log(f"{allowed_num_cores = }")
    num_threads_per_process = int(ceil(num_sources/allowed_num_cores)) # type:ignore

    if num_sources < allowed_num_cores:
        usable_num_cores = num_sources
    else:
        usable_num_cores = ceil(num_sources/num_threads_per_process)
    debug_error_log(f"{usable_num_cores = }")

    if num_threads_per_process > max_threads_per_process:
        debug_error_log((f"Maximum number of threads per process is {max_threads_per_process}. Performance may degrade."))
    return usable_num_cores, num_threads_per_process
    
    # num_process = ceil(num_sources/num_threads_per_process)
    # # validation for maximum core limit
    # allowed_num_cores = os.cpu_count() - 2  # type:ignore
    # if num_process <= allowed_num_cores:
    #     return num_process
    # else:
    #     '''
    #         Need to calculate the number of process after increasing the value of num_threads_per_process.
    #         This increased value must be such that num_process has value just less than num_cores minus 2.
    #         And this value should be updated in configs.json file as well.
    #     '''
    #     cond = True
    #     while cond:
    #         num_threads_per_process += 1
    #         num_process = ceil(num_sources/num_threads_per_process)
    #         if num_process <= allowed_num_cores:
    #             cond = False
    #             return num_process
    #         # number of threads per process limit
    #         if num_threads_per_process > max_threads_per_process:
    #             debug_error_log(f"Maximum number of threads per process is 15")

def update_configs(data:dict, is_source:bool=False):
    configs = load_config(CONFIGS_PATH)

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

    with open(CONFIGS_PATH, 'w', encoding='utf-8') as config_file:
        json.dump(configs, config_file, ensure_ascii=False, indent=4)

def filter_results(results:dict):
    filtered_results = {}
    for i in range(len(results['results'])):
        fingerprinted_confidence = results['results'][i]['fingerprinted_confidence']
        input_confidence = results['results'][i]['input_confidence']
        offset_seconds = results['results'][i]['offset_seconds']
        song_name = results['results'][0]['song_name']

        if fingerprinted_confidence>=0.03 and input_confidence>0.05 and offset_seconds>=0:
        # if input_confidence>0.0 and offset_seconds>=0:
            offset_value = 0.668725
            actual_offset_seconds = round(offset_seconds/offset_value, 2)
            song_id = results['results'][0]['song_id']

            filtered_results[i] = {
                "song_id" : song_id,
                "song_name" : song_name,
                "input_confidence" : input_confidence,
                "fingerprinted_confidence" : fingerprinted_confidence,
                "offset_seconds" : actual_offset_seconds
                
            }
            # debug_error_log(f"validated {song_name=} {input_confidence=} {fingerprinted_confidence=}")
        else:
            pass
            # debug_error_log(f"not validated {song_name=} {input_confidence=} {fingerprinted_confidence=}")
    return filtered_results

def match_audio(djv:Dejavu, filepath:str):
    try:
        results = djv.recognize(
            FileRecognizer, 
            filepath
        )
        # debug_error_log(f'Raw Results: {str(results)}')
    except Exception as e:
        debug_error_log(str(e))
    
    if results['results']:
        results = filter_results(results)
    else:
        results = {}
    # debug_error_log(f'Filtered Results: {str(results)}')
    return results

def db_connection():
    conn = psycopg2.connect(f"""
            dbname={config('PG_DBNAME')} user={config('PG_USER')} password={config('PG_PASSWORD')}
        """)
    return conn

def mysql_db_conn():
    conn = mysql.connector.connect(
        host="localhost",
        user='audio_advertisement',
        password="12345678",
        database="radio"
    )
    return conn

def execute_query(query:str, values:tuple=(), insert:bool=False, req_response:bool=False, top_n_rows:int=-1):
    conn = None
    cur = None
    data = None
    skip_commit = False
    inserted = False
    try:
        # conn = db_connection()
        conn = mysql_db_conn()
    except (Exception) as error:
        debug_error_log(f"Error 1\n{error}")
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        return
        
    try:
        cur = conn.cursor()
        if insert:
            if not values:
                debug_error_log('Values not supplied for query')
                return
            cur.execute(query, values)
            inserted = True
        else:
            if values:
                debug_error_log('Does your query requrires values? ')
                debug_error_log('Executing without using values')
            cur.execute(query)
            skip_commit = True
    except (Exception) as error:
        debug_error_log(f"Error 2\n{error}")
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        return
    
    try:
        # For MySQL DB commit should be omitted for data retival
        if not skip_commit:
            conn.commit()
    except (Exception) as error:
        debug_error_log(f"Error 3\n{error}")
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        return
    
    try:
        # call fucntion to extract response if response required
        if req_response:
            # data = cur.fetchall() if top_n_rows <= 0 else cur.fetchone() if top_n_rows == 1 else cur.fetchmany(size=top_n_rows)
            data = cur.fetchall()
            
    # except (Exception, psycopg2.DatabaseError) as error:
    except (Exception) as error:
        debug_error_log(f"Error 4\n{error}")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
    
    if req_response:
        return data
    if inserted:
        # has less priority than req_response
        return inserted
    return

def create_table():
    query_create_table_advertisements_log = """
        CREATE TABLE IF NOT EXISTS advertisements_log(
            sn SERIAL PRIMARY KEY,
            advert_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            advertisement_id INTEGER NOT NULL,
            log_time TIMESTAMP NOT NULL
        )
    """
    execute_query(query_create_table_advertisements_log)

def get_rel_advert_id(advert_id:int):
    query_select_id = f"""
        SELECT id FROM advertisements 
        WHERE registered_id={advert_id}
    """

    data = execute_query(query_select_id, req_response=True, top_n_rows=1)
    if data:
        rel_advert_id = data[0][0]
    else:
        debug_error_log(f"Can't find advertisement id for registered id {advert_id}")
        rel_advert_id = None
    return rel_advert_id

def check_duration(advert_id:int, channel_id:int):
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

def check_validity(advert_id:int, channel_id:int):
    query_select = f"""
        SELECT validity_from, validity_to FROM advertisement_channel
        WHERE registered_id={advert_id} AND channel_id={channel_id}
    """
    data = execute_query(query_select, req_response=True)

    if data:
        # type is datetime.date
        validity_from, validity_to = data[0][0], data[0][1]
        today = datetime.now().date()
        # date_mapped = today + timedelta(days=3)
        return validity_from <= today <= validity_to, "Invalid date range"
    return False, "No data or channel id and advertisement id mismatch"

def log_needed(advert_id:int, channel_id:int):
    relog_duration = 35
    # check validity
    is_valid, msg = check_validity(advert_id, channel_id)
    if not is_valid:
        debug_error_log(msg)
        return False
    
    duration = check_duration(advert_id, channel_id)
    
    # debug_error_log(f"{duration = }")
    # try:
    #     debug_error_log(f"{duration.seconds = }")
    # except:
    #     pass

    if duration is not None and duration.seconds <= relog_duration:  # duration.total_seconds() returns float precision
        '''No need to log'''
        debug_error_log("last log time validation failed")
        return False
    return True

def keep_log(advert_id:int, channel_id:int, log_dt, input_conf:float, fingerprinted_conf:float, offset_seconds:float):
    log_status = log_needed(advert_id, channel_id)
    debug_error_log(f"{log_status = } for {advert_id = } and {channel_id = }") # store advert id and channel id as well
    rel_advert_id = get_rel_advert_id(advert_id)

    if not log_status or rel_advert_id is None:
        return
    
    # grab channel id from database
    query_insert_log = """
        INSERT INTO advertisements_log(advert_id, channel_id, advertisement_id, log_time, input_conf, fingerprint_conf, offset_sec)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    is_valid_advertisement = execute_query(
        query_insert_log, values=(advert_id, channel_id, rel_advert_id, log_dt, input_conf, fingerprinted_conf, offset_seconds), 
        insert=True
    )
    return is_valid_advertisement

def record_audio(key:str, data:dict, queue:Queue):
    # audio_url, dest_dir, prefix
    return BackgroundRecording(
            data['audio_url'],
            data['dest_dir'],
            data['prefix'], 
            data['bitrate'],
            queue,
            name=key
    )

def dt_from_filepath(filepath:str):
    # regular expression pattern to match the datetime part
    pattern = r'\d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}.\d{6}'
    # Use re.search to find the datetime part in the filename
    match = re.search(pattern, filepath)
    if match:
        # Extract the matched datetime string
        datetime_str = match.group(0)
        
        # Convert the datetime string into a datetime object
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H-%M-%S.%f')
        
        # Now you have a datetime object
        return datetime_obj
    else:
        debug_error_log("Datetime not found in the filename.")
        return None

def delete_file(filepath):
    try:
        os.remove(filepath)
    except:
        debug_error_log(f'audio file {os.path.basename(filepath)} used by another process, unable to remove') 


def logging_removing(results:dict, filepath:str, matching_dir:str):
    # perform database operation
    # print('results ', results)
    if results:
        # debug_error_log(f'Final Results: {str(results)}')
        for result in list(results.values()):
            # print('single result ', result)
            channel_id = get_channel_id(filepath)
            
            log_dt = dt_from_filepath(filepath)

            # extract input and fingerprinted confidence, offset seconds
            input_conf = result['input_confidence']
            fingerprinted_conf = result['fingerprinted_confidence']
            offset_seconds = result['offset_seconds']

            is_valid_advertisement = keep_log(result['song_id'], channel_id, log_dt, input_conf, fingerprinted_conf, offset_seconds)
            # copy audio file to matched folder if matched
            if is_valid_advertisement:
                try:
                    shutil.copy(filepath, matching_dir)
                except Exception as e:
                    debug_error_log(str(e))
            


    # remove the file after matching
    delete_file(filepath)

def init_dejavu(configs):
    dejavu_conf_path = os.path.join(base_dir, configs["rel_dejavu_conf"])
    if not os.path.exists(dejavu_conf_path):
        debug_error_log("Bad dejavu config file path.")
    with open(dejavu_conf_path) as f:
        config = json.load(f)
    try:
        djv = Dejavu(config)
        return djv
    except Exception as e:
        debug_error_log("Can't initiate Dejavu")
        debug_error_log(str(e))
        exit()

def format_db_configs(data:list, audio_dir:str):
    sources = {}
    for row in data:
        source = {}
        id = str(row[0])

        source['audio_url'] = row[1]
        source['bitrate'] = row[2]
        source['dest_dir'] = audio_dir + "/" + id
        source['prefix'] = id + "_clip_"

        sources[id] = source
    # print(f"{sources = }")
    return sources
    
def load_config_db(audio_dir:str):
    query_select_channels = """
        SELECT id, links, bitrate FROM channels
        WHERE status=1
    """
    data = execute_query(query_select_channels, req_response=True)
    # print(f"{data = }")
    return format_db_configs(data, audio_dir=audio_dir)

def matching(filepath:str, djv:Dejavu, matching_dir:str):
    try:
        results = match_audio(djv, filepath)
    except Exception as e:
        debug_error_log(f"Matching error {e}")
        results = None
    finally:
        logging_removing(results, filepath, matching_dir)

def match_residual_audios(djv, matching_dir):
    # if any files are left to match
    # match them by identifying from folder
    residual_audios = glob.glob(configs["base_dir"]+"/audio/recordings/**/*.wav") 
    for residual_audio_filepath in residual_audios:
        matching(residual_audio_filepath, djv, matching_dir)

def process_run(configs:dict, process_num:int, num_threads_per_process:int, sources_keys:list, queue:Queue, djv: Dejavu, matching_dir:str):
    global threads
    # print(process_num)
    keys_idx = process_num*num_threads_per_process
    sources = configs['sources']

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
        stop_thread = stop_recoding(configs['configs_path'])
        for thread in threads:
            for _ in range(thread.queue.qsize()):
                filepath = thread.queue.get()
                # perform audio matching
                if not os.path.exists(filepath):
                    debug_error_log(f"No file named {filepath}")
                    continue
                matching(filepath, djv, matching_dir)

            if stop_thread:
                thread.stop()
                
        if stop_thread:
            # update configs 
            update_configs({"stop_threads": False})
            break
        time.sleep(1.5)

def main(configs:dict, queue:Queue, djv:Dejavu, matching_dir:str):
    match_residual_audios(djv, matching_dir)
    global processes
    update_requires = configs["update"]
    sources = configs['sources']
    num_threads_per_process = configs['num_threads_per_process']
    max_threads_per_process = configs['max_threads_per_process']

    sources_keys = list(configs['sources'].keys())

    num_processes, num_threads_per_process = cores_reqirement(len(sources), num_threads_per_process, max_threads_per_process)

    # processes = []
    for process_num in range(num_processes):
        process = Process(
            target=process_run,
            name='P' + str(process_num), 
            args=(configs, process_num, num_threads_per_process, sources_keys, queue, djv, matching_dir)
        )
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

if __name__=="__main__":
    debug_error_log('---'*15, timestamp=False)
    
    queue = Queue()
    create_table()

    # configurations 
    if not os.path.exists(CONFIGS_PATH):
        debug_error_log("No Config file in the directory")
        raise Exception(f"No Config file in the directory")
    configs = load_config(CONFIGS_PATH)

    base_dir = configs["base_dir"]
    sources = load_config_db(os.path.join(base_dir, configs["rel_rec_dir"]))
    configs['sources'] = sources
    configs['configs_path'] = CONFIGS_PATH

    matching_dir = os.path.join(configs['base_dir'], configs['rel_matched_dir'])
    os.makedirs(matching_dir, exist_ok=True)

    # initialize dejavu
    # dejavu_conf_path = os.path.join(base_dir, configs["rel_dejavu_conf"])
    # if not os.path.exists(dejavu_conf_path):
    #     debug_error_log("Bad dejavu config file path.")
    # with open(dejavu_conf_path) as f:
    #     config = json.load(f)
    # try:
    #     djv = Dejavu(config)
    # except Exception as e:
    #     debug_error_log("Can't initiate Dejavu")
    #     debug_error_log(str(e))
    #     exit()

    djv = init_dejavu(configs)

    # Starting application
    main(configs, queue, djv, matching_dir)

    debug_error_log("Background Threads stopped")
    debug_error_log('---'*15, timestamp=False)
