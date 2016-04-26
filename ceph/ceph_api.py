#!/usr/bin/env python
#coding:utf-8

import os
import sys
import math
import time
import calendar
import threading
import Queue
import boto
import boto.s3.connection
from filechunkio import FileChunkIO
from hashlib import md5

S3_PREFIX = 's3://'
CHUNK_SIZE = 1024*1024*10

def parse_bucket_object(path):
    pather = split_path(path)
    bucket = ""
    key = ""
    if len(pather) > 0:
        bucket = pather[0]
    if len(pather) > 1:
        key += '/'.join(pather[1:])
    if key.startswith("/"):
        print "object name SHOULD NOT begin with /"
        sys.exit(1)
    return (bucket, key)

def split_path(path):
    if not path.lower().startswith(S3_PREFIX):
        print "%s parameter %s invalid, " \
              "must be start with %s" % \
              (args[0], args[1], S3_PREFIX)
        sys.exit(1)
    pather = path[len(S3_PREFIX):].split('/')
    return pather

def convert_to_localtime(s3timestamp, format="%Y-%m-%dT%H:%M:%S.000Z"):
    ts = format_unixtime(s3timestamp, format)
    return time.strftime("%Y-%m-%d %X", time.localtime(ts))

def format_unixtime(s3timestamp, format="%Y-%m-%dT%H:%M:%S.000Z"):
    ts = (int)(calendar.timegm(time.strptime(s3timestamp,format)))
    return ts

class ceph_api(object):
    def conn(self,**args):
        if "id" not in args:
            print "param id is not null"
            sys.exit(1)
        if "key" not in args:
            print "param key is not null"
            sys.exit(1)
        if "host" not in args:
            print "param host is not null"
            sys.exit(1)

        access_id = args["id"]
        access_key = args["key"]
        host = args["host"]
        port = 80
        ssl = False
        if "port" in args:
            port = args["port"]

        if "ssl" in args:
            ssl = True

        conn = boto.connect_s3(
            aws_access_key_id = access_id,
            aws_secret_access_key = access_key,
            host = host,
            port = port,
            is_secure = ssl,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
        return ceph_cmd(conn)

class ceph_cmd(object):
    conn = None
    def __init__(self,conn):
        self.conn = conn

    def uploadfrompost(self,args,options,view_bar=True):
        content = args[1]
        (bucket,key) = parse_bucket_object(args[2])

        if not key:
            key = os.path.basename(srcFile)

        if not content:
            print "Content is not none"
            exit(-1)

        cb = None
        if view_bar:
            cb = self._view_bar

        from boto.s3.key import Key
        try:
            b = self.conn.get_bucket(bucket)
            k = Key(b)
            k.key = key
            hash_obj = md5()
            hash_obj.update(content)
            k.etag = hash_obj.hexdigest()
            res = k.set_contents_from_string(content,cb=cb)
            if res is not None:
                res = "\n"
                res += "Object URL is: "+self._url(bucket,key)+"\n"
                res += "Object abstract path is: "+os.path.join(args[2],key)+"\n"
                res += "ETag is "+k.etag
                return res
        except boto.exception.S3ResponseError as e:
            return "Error Status:\n%d %s" % (e.status,e.reason)

    def upload(self,args,options,view_bar=True):
        srcFile = args[1]
        (bucket,key) = parse_bucket_object(args[2])

        if not key:
            key = os.path.basename(srcFile)

        if not os.path.isfile(srcFile):
            print "%s is not file, please input localfile" % srcFile
            exit(-1)

        (status,is_replace,body) = self._upload(srcFile,bucket,key,view_bar)
        if status == 200:
            res = "\n"
            res += "Object URL is: "+self._url(bucket,key)+"\n"
            res += "Object abstract path is: "+os.path.join(args[2],key)+"\n"
            res += "ETag is "+body["etag"]
            return res
        else:
            return "Error Status:\n%d %s" % (status,body["reason"])

    def uploadfromdir(self,args,options):
        localdir = args[1]
        (bucket,prefix) = parse_bucket_object(args[2])
        if not os.path.isdir(localdir):
            print "%s is not dir, please input localdir" % localdir
            exit(-1)

        if not localdir.endswith(os.sep):
            localdir = "%s%s" % (localdir, os.sep)

        thread_num = 5
        if options.thread_num:
            thread_num = (int)(options.thread_num)

        is_replace = False
        if options.replace and options.replace.lower() == "true":
            is_replace = True

        queue = Queue.Queue(0)

        def process_put(items):
            for item in items:
                local_file = os.path.join(root,item)
                if os.path.isfile(local_file):
                    local_file_full_path = os.path.abspath(local_file)
                    object_prefix = os.path.join(prefix,root.replace(localdir,""))
                    object_full_path = os.path.join(object_prefix,item)
                    queue.put((self._upload,local_file_full_path,bucket,object_full_path,is_replace))

            total = len(items)
            success = 0
            skip = 0
            thread_pool = []
            for i in xrange(thread_num):
                thread = UploadFileWord(queue)
                thread.start()
                thread_pool.append(thread)

            for thread in thread_pool:
                thread.join()
                (ok_num,skip_num) = thread.ok()
                success += ok_num
                skip += skip_num

            queue.join()
            return total,success,skip

        total = 0
        success = 0
        skip = 0
        for root,dirs,files in os.walk(localdir):
            process_put(dirs)
            (tmp_total,tmp_success,tmp_skip) = process_put(files)
            total += tmp_total
            success += tmp_success
            skip += tmp_skip
        res = "\n"
        res += "Total being uploaded localfiles num: %d\n" % total
        res += "OK num:%d, SKIP num:%d, FAIL num:%d" % (success,skip,(total - success - skip))
        return res

    def delete(self,args,options):
        (bucket,key) = parse_bucket_object(args[1])
        try:
            b = self.conn.get_bucket(bucket)
            k = b.lookup(key)
            if k is not None:
                k.delete()
        except boto.exception.S3ResponseError as e:
            return "Error Status:\n%d %s" % (e.status, e.reason)

    def deleteallobject(self,args,options):
        (bucket,prefix) = parse_bucket_object(args[1])

        force_delete = False
        if options.force and options.force.lower() == "true":
            force_delete = True

        marker = ""
        if options.marker:
            marker = options.marker

        thread_num = 5
        if options.thread_num:
            thread_num = (int)(options.thread_num)

        if not force_delete:
            ans = raw_input("DELETE all objects? y/N, default is N: ")
            if ans.lower() != "y":
                print "quit."
                exit(-1)

        queue = Queue.Queue(0)

        try:
            b = self.conn.get_bucket(bucket)
            total = 0
            success = 0
            while True:
                get_all_keys = b.get_all_keys(prefix=prefix,marker=marker)
                for k in get_all_keys:
                    total += 1
                    marker = k.name
                    queue.put((k.delete))

                if len(get_all_keys) == 0:
                    break

            thread_pool = []
            for i in xrange(thread_num):
                thread = RemoveFileWord(queue)
                thread.start()
                thread_pool.append(thread)

            for thread in thread_pool:
                thread.join()
                success += thread.ok()

            queue.join()

            if success:
                #res = "delete_all_objects: Now %d objects deleted\n" % total
                #res += "OK num:%d\n" % success
                #res += "FAIL num:%d" % (total - success)
                #return res
                res = "\n"
                res += "delete_all_objects: Now %d objects deleted" % success
                return res
        except boto.exception.S3ResponseError as e:
            return "Error Status:\n%d %s" % (e.status, e.reason)

    def list(self,args,options):
        (bucket,prefix) = parse_bucket_object(args[1])

        delimiter = ""
        if options.delimiter:
            delimiter = options.delimiter

        marker = ""
        if options.marker:
            marker = options.marker

        maxkeys = 1000
        if options.maxkeys:
            maxkeys = options.maxkeys

        try:
            b = self.conn.get_bucket(bucket)
            pl = "prefix list is:\n"
            ol = "object list is:\n"
            pl_num = 0
            ol_num = 0
            for k in b.get_all_keys(prefix=prefix,delimiter=delimiter,marker=marker,maxkeys=maxkeys):
                if hasattr(k,"CommonPrefixes"):
                    pl += "%s\n" % k.name
                    pl_num += 1
                else:
                    k.last_modified = convert_to_localtime(k.last_modified)
                    ol += "%s %s %s %s\n" % (k.last_modified,k.size,(k.storage_class).title(),os.path.join(S3_PREFIX,bucket,k.name))
                    ol_num += 1
            yield "%s%s\nprefix list number is: %d\nobject list number is: %d" % (pl,ol,pl_num,ol_num)
        except boto.exception.S3ResponseError as e:
            yield "Error Status:\n%d %s" % (e.status,e.reason)

    def meta(self,args,options):
        (bucket,key) = parse_bucket_object(args[1])
        try:
            b = self.conn.get_bucket(bucket)
            k = b.get_key(key)
            if k is not None:
                res = "objectname: %s\n" % k.name
                res += "content-length: %d\n" % k.size
                res += "accept-ranges: bytes\n"
                res += "connection: keep-alive\n"
                res += "server: s3\n"
                res += "last-modified: %s\n" % k.last_modified
                res += "etag: %s\n" % k.etag
                res += "date: %s\n" % k.date
                res += "content-type: %s" % k.content_type
                return res
            else:
                return "404 Not found"
        except boto.exception.S3ResponseError as e:
            return "Error Status:\n%d %s" % (e.status,e.reason)

    def cat(self,args,options):
        (bucket,key) = parse_bucket_object(args[1])
        try:
            b = self.conn.get_bucket(bucket)
            k = b.lookup(key)
            while True:
                data = k.read(10240)
                if len(data) != 0:
                    yield data
                else:
                    break
        except boto.exception.S3ResponseError as e:
            yield "Error Status:\n%d %s" % (e.status, e.reason)

    def copy(self,args,options):
        (source_bucket,source_key) = parse_bucket_object(args[1])
        (target_bucket,target_key) = parse_bucket_object(args[2])

        try:
            sb = self.conn.get_bucket(source_bucket)
            sk = sb.get_key(source_key)
            tk = sk.copy(target_bucket,target_key)
            if tk is not None:
                res = "Object URL is: %s\n" % self._url(target_bucket,target_key)
                res += "Object abstract path is: %s\n" % args[2]
                res += "ETag is \"%s\"" % tk.etag
                return res
        except AttributeError as e:
            return "%s" % e
        except boto.exception.S3ResponseError as e:
            return "Error Status:\n%d %s" % (e.status, e.reason)

    def copylarge(self,args,options):
        pass

    def copybucket(self,args,options):
        (source_bucket,source_prefix) = parse_bucket_object(args[1])
        (target_bucket,target_prefix) = parse_bucket_object(args[2])

        marker = ""
        if options.marker:
            marker = options.marker

        thread_num = 5
        if options.thread_num:
            thread_num = (int)(options.thread_num)

        is_replace = False
        if options.replace and options.replace.lower() == "true":
            is_replace = True

        queue = Queue.Queue(0)

        try:
            sb = self.conn.get_bucket(source_bucket)
            total = 0
            success = 0
            skip = 0
            while True:
                get_all_keys = sb.get_all_keys(prefix=source_prefix,marker=marker)
                for sk in get_all_keys:
                    total += 1
                    marker = sk.name
                    target_key = os.path.join(target_prefix,os.path.basename(sk.name))
                    #sk = sb.get_key(sk.name)
                    queue.put((self._copy,sk,target_bucket,target_key,is_replace))

                if len(get_all_keys) == 0:
                    break

            thread_pool = []
            for i in xrange(thread_num):
                thread = CopyFileWord(queue)
                thread.start()
                thread_pool.append(thread)

            for thread in thread_pool:
                thread.join()
                (ok_num,skip_num) = thread.ok()
                success += ok_num
                skip += skip_num

            queue.join()

            res = "Total being copied objects num: %d, from %s to %s\n" % (total,args[1],args[2])
            res += "OK num:%d\n" % (success + skip)
            #res += "SKIP num:%d\n" % skip
            res += "FAIL num:%d" % (total - success - skip)
            return res
        except boto.exception.S3ResponseError as e:
            return "Error Status:\n%d %s" % (e.status, e.reason)

    def uploadlarge(self,args,options):
        srcFile = args[1]
        (bucket,key) = parse_bucket_object(args[2])

        if not os.path.isfile(srcFile):
            print "%s is not file, please input localfile" % srcFile
            exit(-1)

        if not key:
            key = os.path.basename(srcFile)

        thread_num = 5
        if options.thread_num:
            thread_num = (int)(options.thread_num)

        source_size = os.path.getsize(srcFile)
        chunk_count = int(math.ceil(source_size / float(CHUNK_SIZE)))

        b = self.conn.get_bucket(bucket)
        mp_upload = b.initiate_multipart_upload(key)

        queue = Queue.Queue(0)

        for i in xrange(chunk_count):
            offset = CHUNK_SIZE * i
            bytes = min(CHUNK_SIZE, source_size - offset)
            fp = FileChunkIO(srcFile,"r",offset=offset,bytes=bytes)
            queue.put((mp_upload.upload_part_from_file,fp,i+1))

        thread_pool = []
        for i in xrange(thread_num):
            thread = UploadPartWord(queue)
            thread.start()
            thread_pool.append(thread)

        for thread in thread_pool:
            thread.join()

        queue.join()

        body = mp_upload.complete_upload()
        res = "Object URL is: %s\n" % self._url(bucket,key)
        res += "Object abstract path is: %s\n" % os.path.join(S3_PREFIX,bucket,key)
        res += "ETag is \"%s\"" % body.etag
        return res

    def _url(self,bucket,key):
        bucket = self.conn.get_bucket(bucket)
        k = bucket.get_key(key)
        if k is not None:
            k.set_canned_acl('public-read')
            return k.generate_url(0,query_auth=False,force_http=False)

    def _upload(self,srcFile,bucket,key,view_bar=True,is_replace=True):
        cb = None
        if view_bar:
            cb = self._view_bar

        if not is_replace:
            local_file_size = os.path.getsize(srcFile)
            local_modify_time = os.path.getmtime(srcFile)
            res = self._head(bucket,key)
            if res and local_file_size == res.size:
                format = "%a, %d %b %Y %H:%M:%S GMT"
                s3_last_modify_time = format_unixtime(res.last_modified,format)
                if s3_last_modify_time >= local_modify_time:
                    return (200,False,{"etag":res.etag})

        fp = open(srcFile,'rb')
        from boto.s3.key import Key
        try:
            b = self.conn.get_bucket(bucket)
            k = Key(b)
            md5 = k.compute_md5(fp)
            k.key = key
            k.etag = md5[0]
            res = k.set_contents_from_file(fp,cb=cb)
            if res is not None:
                return (200,True,{"etag":k.etag})
        except boto.exception.S3ResponseError as e:
            return (e.status,True,{"reason":e.reason})

    def _copy(self,sk,target_bucket,target_key,is_replace=True):
        try:
            if not is_replace:
                source_key_size = sk.size
                source_key_time = format_unixtime(sk.last_modified)
                res = self._head(target_bucket,target_key)
                if res and source_key_size == res.size:
                    format = "%a, %d %b %Y %H:%M:%S GMT"
                    s3_last_modify_time = format_unixtime(res.last_modified,format)
                    if s3_last_modify_time >= source_key_time:
                        return (200,False,{"etag":res.etag})

            res = sk.copy(target_bucket,target_key)
            if res is not None:
                return (200,True,{"etag":res.etag})
        except boto.exception.S3ResponseError as e:
            return (e.status,True,{"reason":e.reason})

    def _head(self,bucket,key):
        b = self.conn.get_bucket(bucket)
        k = b.get_key(key)
        return k

    def _view_bar(self,num,total):
        rate = float(num) / float(total)
        rate_num = int(rate * 100)
        print "\r%d%% " % rate_num,
        sys.stdout.flush()


class UploadFileWord(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.ok_num = 0
        self.skip_num = 0

    def run(self):
        while True:
            try:
                (ceph_cmd,srcFile,bucket,key,is_replace) = self.queue.get(block=False)
                (status,is_replace,body) = ceph_cmd(srcFile,bucket,key,view_bar=False,is_replace=is_replace)
                if status == 200:
                    if is_replace:
                        self.ok_num += 1
                    else:
                        self.skip_num += 1

                self.queue.task_done()
            except Queue.Empty:
                break

    def ok(self):
        return self.ok_num,self.skip_num

class UploadPartWord(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.ok_num = 0

    def run(self):
        while True:
            try:
                (ceph_cmd,fp,part_num) = self.queue.get(block=False)
                res = ceph_cmd(fp,part_num=part_num)
                print res
                if res is not None:
                    self.ok_num += 1

                self.queue.task_done()
            except Queue.Empty:
                break

    def ok(self):
        return self.ok_num

class CopyFileWord(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.ok_num = 0
        self.skip_num = 0

    def run(self):
        while True:
            try:
                (ceph_cmd,sk,target_bucket,target_key,is_replace) = self.queue.get(block=False)
                (status,is_replace,body) = ceph_cmd(sk,target_bucket,target_key,is_replace)
                if status == 200:
                    if is_replace:
                        self.ok_num += 1
                    else:
                        self.skip_num += 1

                self.queue.task_done()
            except Queue.Empty:
                break

    def ok(self):
        return self.ok_num,self.skip_num

class RemoveFileWord(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.ok_num = 0

    def run(self):
        while True:
            try:
                (ceph_cmd) = self.queue.get(block=False)
                res = ceph_cmd()
                if res is not None:
                    self.ok_num += 1

                self.queue.task_done()
            except Queue.Empty:
                break

    def ok(self):
        return self.ok_num
