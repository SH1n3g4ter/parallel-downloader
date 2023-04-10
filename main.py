import os
import sys
import eventlet
eventlet.monkey_patch(socket=True)
from eventlet.timeout import Timeout
import requests
import threading
import shutil
#import urllib.request
import timeit
import time
import math
import base64
import queue
import crcmod
import uuid
#import socks, socket

def monkeypatch_method(cls):
    def decorator(func):
        setattr(cls, func.__name__, func)
        return func
    return decorator

@monkeypatch_method(requests.Response)
def iter_content_with_timeout(self, **kwargs):
    if 'timeout' in kwargs:
        timeout = kwargs.pop('timeout')
    else:
        raise TypeError('timeout not provided')
    it = self.iter_content(**kwargs)
    try:
        while True:
            with Timeout(timeout):
                yield next(it)
    finally:
        it.close()

class Downloader:
    class Item:
        """Job queue item class"""
        def __init__(self, chunk_id, chunk_range, proxy_dict, url=None, was_interrupted=False):
            self.chunk_id = chunk_id  # chunk id to be downloaded
            self.chunk_range = chunk_range  # chunk range to download from server
            self.was_interrupted = was_interrupted  # flag to denote if the job was interrupted due to some error
            self.proxy_dict = proxy_dict
            self.url=url
            
    def get_requests_proxy_config(self, port_offset=0):
        if self.tor_socks5 == None: return None
        tor_ip = self.tor_socks5["ip"]
        tor_port = int(self.tor_socks5["port"])
        return {
            'http': f"socks5://{tor_ip}:{tor_port+port_offset}",
            'https': f"socks5://{tor_ip}:{tor_port+port_offset}"
        }

    def __init__(self, url=None, number_of_threads=1, name="", verify_cert=True, tor_socks5=None, multipath=None):
        """Constructor of Downloader class
        :param url: URL of file to be downloaded (optional)
        :param number_of_threads: Maximum number of threads (optional)
        """
        self.tor_socks5 = tor_socks5
        self.url = url  # url of a file to be downloaded
        self.number_of_threads = number_of_threads  # maximum number of threads
        if multipath:
            with open(multipath, 'r') as file:
                self.multiurl = [line.rstrip() for line in file]
            self.url = self.multiurl[0]
            self.multi_reserve = int(0.1*len(self.multiurl))
            self.number_of_threads = len(self.multiurl)-self.multi_reserve
            self.tail_pos = 0
        else:
            self.multiurl = None
        self.verify_cert = verify_cert
        self.file_size = self.get_file_size()  # remote file's size
        self.if_byte_range = self.is_byte_range_supported()  # if remote server supports byte range
        self.remote_crc32c = self.get_remote_crc32c()  # remote file's checksum
        self.if_contains_crc32c = True if self.remote_crc32c != -1 or self.remote_crc32c is not None else False  # if remote file has a checksum
        self.downloaded_crc32c = None  # checksum of a downloaded file
        self.range_list = list()  # byte range for each download thread
        self.start_time = None  # start time to calculate overall download time
        self.end_time = None  # end time to calculate overall download time
        self.target_filename = os.path.basename(self.url) if name == "" else name # name of a file to be downloaded
        self.status_refresh_rate = 2  # status will be refreshed after certain time (in seconds)
        self.download_durations = [None] * self.number_of_threads  # total download time for each thread (for benchmarking)
        self.q = queue.Queue(maxsize=0)  # worker threads will pick download job from the queue
        self.append_write = "wb"  # default mode will be write in binary
        self.download_status = list()  # current download job status of each thread (for benchmarking)
        self.current_status = ""  # current overall status
        self.start_offset = 0
        self.tmp_id = str(uuid.uuid4())
        self.overwrite_byte_range = False

    def get_url(self):
        """Returns URL of a file to be downloaded"""
        return self.url

    def set_url(self, url):
        """Set new URL of a file to be downloaded
        :param url: string
        """
        if not url:
            raise ValueError("URL field is empty")
        if not isinstance(url, str):
            raise TypeError("URL must be of string type")
        self.url = url

    def get_number_of_threads(self):
        """Returns maximum number of threads allowed"""
        return self.number_of_threads

    def set_number_of_threads(self, number_of_threads):
        """Set new maximum number of threads allowed (must be a positive number)
        :param number_of_threads: integer
        """
        if number_of_threads <= 0:
            raise ValueError("Number of maximum threads should be positive")
        if not isinstance(number_of_threads, int):
            raise TypeError("Number of maximum threads should be integer")
        self.number_of_threads = number_of_threads

    def get_file_size(self):
        """Get remote file size in bytes from url
        :return: integer
        """
        proxy_dict = self.get_requests_proxy_config()
        self.file_size = requests.head(self.url, headers={'Accept-Encoding': 'identity'}, verify=self.verify_cert, proxies=proxy_dict).headers.get('content-length', None)
        return int(self.file_size)

    def is_byte_range_supported(self):
        """Return True if accept-range is supported by the url else False
        :return: boolean
        """
        server_byte_response = requests.head(self.url, headers={'Accept-Encoding': 'identity'}, verify=self.verify_cert, proxies=self.get_requests_proxy_config()).headers.get('accept-ranges')
        if not server_byte_response or server_byte_response == "none":
            return False
        else:
            return True

    def is_contains_crc32c(self):
        return self.if_contains_crc32c

    def get_remote_crc32c(self):
        server_crc32c_response = requests.head(self.url, headers={'Accept-Encoding': 'identity'}, verify=self.verify_cert, proxies=self.get_requests_proxy_config()).headers.get(
            'x-goog-hash')
        if server_crc32c_response:
            response_split = server_crc32c_response.split(', ')
            for response in response_split:
                if response.startswith("crc32c"):
                    return response[7:]
        return None

    def start_download(self):
        """If byte range is supported by server, perform below steps:
            1. Delete temp folder if exists
            2. Fill queue with number of jobs = number of threads
            3. Start worker threads
            4. Keep checking status until all worker downloads reach 100%
            5. Wait till all download complete
            6. Merge chunks of files into a single file
            7. Delete temp folder

        If byte range is not supported server, download file entirely.
        """
        self.start_time = timeit.default_timer()

        print("Server support byte range GET? ... ", end="")

        if self.if_byte_range or self.overwrite_byte_range:
            print("Yes")
            if not os.path.isdir("temp"):
                os.makedirs("temp")

            self.fill_initial_queue()
            print(obj.get_metadata())

            print("Starting download threads ... ", end="")
            for i in range(self.number_of_threads):
                worker = threading.Thread(target=self.download_chunk)
                worker.setDaemon(True)
                worker.start()
            print("Done")

            print(self.get_status_header())
            while self.get_download_status():
                print(self.current_status)
                time.sleep(self.status_refresh_rate)

            self.q.join()

            print(self.current_status)

            print("File chunks download ... Done")
            print("Merging chunks into a single file ... ", end="")
            with open(self.target_filename, "ab") as target_file:
                for i in range(self.number_of_threads):
                    partpath = f"temp/part{str(i)}_{self.tmp_id}_{self.target_filename}"
                    with open(partpath, "rb") as chunk_file:
                        target_file.write(chunk_file.read())
                    os.unlink(partpath)
            print("Done")

        else:
            print("No")
            try:
                if self.start_offset != 0:
                    print("cannot download at given offset")
                print("Download file at once ... ", end="")
                self.download_entire_file()
                print("Done")
            except:
                print("Error occurred while downloading.")

        print("Checking integrity ... ", end="")
        integrity_result = self.check_integrity()
        if not self.remote_crc32c:
            print("Checksum is not available for a remote file. Integrity check cannot be performed.")
        elif integrity_result:
            print("Successful")
        else:
            print("Failed")

        self.end_time = timeit.default_timer()

        print("Displaying benchmarks ... ")
        self.display_benchmarks()

    def fill_initial_queue(self):
        """Fill the queue at the start of downloading"""
        self.build_range()
        for chunk_id, chunk_range in enumerate(self.range_list):
            self.q.put(self.Item(chunk_id, chunk_range, self.get_requests_proxy_config(chunk_id), self.multiurl[chunk_id] if self.multiurl else None, False))

    def download_chunk(self):
        """Get job from queue. Download chunk of a file. Range is extracted from job's chunk_range field"""
        while True:
            item = self.q.get()
            try:
                if item.was_interrupted:
                    time.sleep(1)
                    if os.path.isfile(f"temp/part{str(item.chunk_id)}_{self.tmp_id}_{self.target_filename}"):
                        self.append_write = "ab"
                        temp = item.chunk_range.split('-')
                        item.chunk_range = str(int(temp[0]) + os.stat(f"temp/part{str(item.chunk_id)}_{self.tmp_id}_{self.target_filename}").st_size) + '-' + temp[1]
                    else:
                        self.append_write = "wb"

                if self.multiurl is None:
                    print("proxy dict none")
                    url = self.get_url()
                else:
                    url = item.url
                
                #TODO combine this with the interrupt part above
                partpath = f"temp/part{str(item.chunk_id)}_{self.tmp_id}_{self.target_filename}"
                with open(partpath, self.append_write) as out_file:
                    if self.multiurl is None:
                        req = requests.get(url, stream=True, headers={'Range':f"bytes={item.chunk_range}"}, verify=self.verify_cert, proxies=item.proxy_dict)
                        for chunk in req.iter_content(chunk_size=1024):
                            if chunk:
                                out_file.write(chunk)
                    else:
                        clear_to_pass = False
                        while not clear_to_pass: #scuffed af to use tor connections at end of port range
                            req = requests.get(url, stream=True, headers={'Range':f"bytes={item.chunk_range}"}, verify=self.verify_cert, proxies=item.proxy_dict)
                            if req.status_code != 200 and req.status_code != 206:
                                print(f"BAD response code ({req.status_code}) for item "+str(item.chunk_id))
                                item.proxy_dict = self.get_requests_proxy_config(127-self.tail_pos)
                                self.tail_pos = self.tail_pos + 1
                                print(f"proxy config:{str(item.proxy_dict)}")
                                print(f"tail now at:{127-self.tail_pos}")
                            else:
                                try:
                                    for chunk in req.iter_content_with_timeout(chunk_size=1024, timeout=5.0):
                                        if chunk:
                                            out_file.write(chunk)
                                    clear_to_pass = True
                                except Timeout:
                                    print(f"no content received in timeout [chunk_id:{item.chunk_id}]")
                                    if self.multi_reserve > 0:
                                        print("switching to reserve")
                                        range_start, range_end = item.chunk_range.split('-')
                                        out_file.flush()
                                        os.fsync(out_file.fileno())
                                        time.sleep(1)
                                        sofar_size = int(os.path.getsize(partpath))
                                        item.chunk_range = f"{int(range_start)+sofar_size}-{range_end}"
                                        item.url = self.multiurl[len(self.multiurl)-self.multi_reserve]
                                        url = item.url
                                        self.multi_reserve = self.multi_reserve - 1
                                        print(f"new chunk range: {item.chunk_range}")
                                        print(f"{self.multi_reserve} remaining reserves")
                                        item.proxy_dict = self.get_requests_proxy_config(127-self.tail_pos)
                                        self.tail_pos = self.tail_pos + 1
                                        print(f"proxy config:{str(item.proxy_dict)}")
                                        print(f"tail now at:{127-self.tail_pos}")
                                    else:
                                        print("ERR no reserves remaining")
                                        clear_to_pass = True
                                except (StopIteration, Exception):
                                    clear_to_pass = True
                self.download_durations[item.chunk_id] = timeit.default_timer()

            except IOError:
                item.was_interrupted = True
                self.q.put(item)

            finally:
                self.q.task_done()

    def download_entire_file(self):
        """If byte range is not supported by server, download entire file"""
        req = requests.get(self.url, stream=True, verify=self.verify_cert, proxies=self.get_requests_proxy_config())
        with open(self.target_filename, 'wb') as out_file:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    out_file.write(chunk)

    def get_status_header(self):
        """Returns header for the download status"""
        status_header = list()
        for i in range(self.number_of_threads):
            status_header.append("chunk" + str(i+1))
        return '\t\t'.join(status_header)

    def get_download_status(self):
        """Returns current download status per thread separated by tabs in a string format
        :return: string
        """
        self.download_status.clear()
        for i in range(self.number_of_threads):
            if os.path.isfile(f"temp/part{str(i)}_{self.tmp_id}_{self.target_filename}"):
                self.download_status.append(str(round(os.stat(f"temp/part{str(i)}_{self.tmp_id}_{self.target_filename}").st_size/((self.file_size-self.start_offset)/self.number_of_threads) * 100, 2)) + "%")
            else:
                self.download_status.append("0.00% [nofile]")
        self.current_status = '\t\t'.join(self.download_status)
        if all(x == "100.0%" for x in self.download_status):
            return False
        else:
            return True

    def display_benchmarks(self):
        """Disply benchmark results"""
        print("\nBenchmarking Results:")
        print("\nTotal time taken for download and integrity check:", round(self.end_time - self.start_time, 2), "seconds.")
        if self.if_byte_range:
            print("\nThread\t\tTime Taken\t\tAverage Download Speed")
            for i in range(self.number_of_threads):
                total_time = self.download_durations[i] - self.start_time
                average_speed = (((self.file_size-self.start_offset) / self.number_of_threads) / total_time) * (8 / (1024 * 1024))
                print(i+1, "\t\t", round(total_time, 2), "seconds\t\t", round(average_speed, 2), "mbps")

    def get_downloaded_crc32c(self):
        """Compute and returns crc32c checksum of a downloaded file
        :return: string
        """
        file_bytes = open(self.target_filename, 'rb').read()
        crc32c = crcmod.predefined.Crc('crc-32c')
        crc32c.update(file_bytes)
        crc32c_value = base64.b64encode(crc32c.digest())
        self.downloaded_crc32c = str(crc32c_value, 'utf-8')
        return self.downloaded_crc32c

    def check_integrity(self):
        self.get_downloaded_crc32c()
        return self.remote_crc32c == self.downloaded_crc32c

    def build_range(self):
        """Creates the list of byte-range to be downloaded by each thread.
        Total file size is divided by maximum limit of a thread
        """
        i = self.start_offset
        chunk_size = int(math.ceil((int(self.file_size)-self.start_offset) / int(self.number_of_threads)))
        print(f"chunk_size: {chunk_size}")
        for _ in range(self.number_of_threads):
            if(i + chunk_size) < self.file_size:
                entry = '%s-%s' % (i, i + chunk_size - 1)
            else:
                entry = '%s-%s' % (i, self.file_size)
            i += chunk_size
            self.range_list.append(entry)
        print(f"range list: {self.range_list}")

    def get_target_filename(self):
        """Returns the target file name"""
        return self.target_filename

    def get_metadata(self):
        """Returns object metadata information"""
        return {
            "url": self.url,
            "number_of_threads": self.number_of_threads,
            "file_size": self.file_size,
            "if_byte_range": self.if_byte_range,
            "if_contains_crc32c": self.if_contains_crc32c,
            "remote_crc32c": self.remote_crc32c,
            "downloaded_crc32c": self.downloaded_crc32c,
            "range_list": self.range_list,
            "offset": self.start_offset,
            "uuid":self.tmp_id
        }


def getopts(argv):
    """Parse arguments passed and add them to dictionary
    :param argv: arguments must be url and threads
    :return: dictionary of arguments
    """
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found an option
            if len(argv) > 1 and argv[1][0] != '-': #found "-name value" pair.
                opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
            else: #found option that doesnt need value
                opts[argv[0]] = None
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts


if __name__ == '__main__':
    """Instead of command line arguments, the script can be run by creating a Downloader object as well.
    For ex.:
        1. obj = Downloader("https://storage.googleapis.com/vimeo-test/work-at-vimeo-2.mp4", 10)
        2. obj = Downloader("http://i.imgur.com/z4d4kWk.jpg", 3)
        
    Once objects are created, call start_download function as obj.start_download()
    """ 

    url = ""
    threads = ""
    name = ""
    verify_cert = True
    arguments_list = getopts(sys.argv)
    tor_proxy = None
    multipath = None
    if '-tor' in arguments_list:
        print("setting socks5 tor proxy")
        if arguments_list['-tor'] is None:
            tor_proxy = {"ip":"127.0.0.1","port":"9000"}
        else:
            tor_proxy = {"ip":arguments_list['-tor'].split(":")[0],"port":arguments_list['-tor'].split(":")[1]}
    if '-url' in arguments_list:
        url = arguments_list['-url']
    if '-threads' in arguments_list:
        threads = int(arguments_list['-threads'])
    if '-name' in arguments_list:
        name = arguments_list['-name']
    if '-noverify' in arguments_list:
        verify_cert = False
    if '-multipath' in arguments_list:
        multipath = arguments_list['-multipath']

    if (not url or not threads) and not multipath:
        raise ValueError("Please provide required arguments.")
    if multipath and not tor_proxy:
        raise ValueError("multipath requires '-tor'")

    obj = Downloader(url, threads, name, verify_cert, tor_proxy, multipath)
    # obj = Downloader("https://storage.googleapis.com/vimeo-test/work-at-vimeo-2.mp4", 10)
    # obj = Downloader("http://i.imgur.com/z4d4kWk.jpg", 3)
    
    if '-offset' in arguments_list:
        obj.start_offset = int(arguments_list['-offset'])
        print(f"attempting download at offset {obj.start_offset}")
    if '-overwritebr' in arguments_list:
        obj.overwrite_byte_range = True
        print("overwriting byte range")
        if '-filesize' not in arguments_list:
            print("YOU SHOULD PROBABLY ALSO SET '-filesize'")
    if '-filesize' in arguments_list:
        obj.file_size = int(arguments_list['-filesize'])
    
    obj.start_download()
    print(obj.get_remote_crc32c())
    print(obj.get_downloaded_crc32c())
