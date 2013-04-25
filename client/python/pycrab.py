import time
import sys
import struct
import socket
import select
import msgpack
import zlib

REQUEST_FLAG_NO_REPLY       =  1 << 0
REQUEST_FLAG_GZIP           =  1 << 1

RESPONSE_STATUS_SUCCESS     =  0
RESPONSE_STATUS_REDIRECTION =  1

class Table:
    def __init__(self, bucket, table_id):
        self.bucket = bucket
        self.table_id = table_id
    def insert(self, data, **kwargs):
        flags = 0
        
        if 'noreply' in kwargs:
            if kwargs['noreply']:
                flags |= REQUEST_FLAG_NO_REPLY
            del kwargs['noreply']

        params = kwargs
        params.update({
            'bucket': self.bucket.name,
            'table': self.table_id,
            "data": data,
        })
        
        return Command(self.bucket.connection, 4, flags, params).execute()
    def find(self, *args, **kwargs):
        if isinstance(self.table_id, (int, long)):
            kwargs.update({
                'bucket': self.bucket.name,
                'table': self.table_id,
            })
            if len(args) == 1:
                kwargs['query'] = args[0]
            elif len(args) > 1:
                kwargs['query'] = (['$and'] + list(args))
            return Command(self.bucket.connection, 1, 0, kwargs)
        else:
            kwargs.update({
                'bucket': self.bucket.name,
                'tables': self.table_id,
            })
            if len(args) == 1:
                kwargs['query'] = args[0]
            elif len(args) > 1:
                kwargs['query'] = (['$and'] + list(args))
            return Command(self.bucket.connection, 9, 0, kwargs)
            
    def __getitem__(self, pk):
        payload = {
            'bucket': self.bucket.name,
            'table': self.table_id,
        }
        payload['pk'] = pk
        return Command(self.bucket.connection, 1, 0, payload).first()
    def __contains__(self, pk):
        return bool(self[pk])
    def __iter__(self):
        return [].__iter__()
    def remove(self, query={}, **kwargs):

        flags = 0
        if 'noreply' in kwargs:
            if kwargs['noreply']:
                flags |= REQUEST_FLAG_NO_REPLY
            del kwargs['noreply']

        return Command(self.bucket.connection, 2, flags, {
            'bucket': self.bucket.name,
            'table': self.table_id,
            'query': query,
        }).execute()

    def get_raw(self):
        return Command(self.bucket.connection, 16, 0, {
            'bucket': self.bucket.name,
            'table': self.table_id,
        }).execute()

    def set_raw(self, data, **kwargs):

        flags = 0
        if 'noreply' in kwargs:
            if kwargs['noreply']:
                flags |= REQUEST_FLAG_NO_REPLY
            del kwargs['noreply']

        return Command(self.bucket.connection, 17, flags, {
            'bucket': self.bucket.name,
            'table': self.table_id,
            'data': data,
        }).execute()

    def __repr__(self):
        return '<Table %s[%d] with %d records>' % (self.bucket.name, self.table_id, self.find().count())

class Bucket:
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name
    def __getitem__(self, table_id):
        return Table(self, table_id)
    def set_fields(self, fields):
        fields_d = []
        for field in fields:
            fi = {
                'name': field.name,
                'nbits': field.nbits,
            }
            if field.kwargs:
                for k in field.kwargs:
                    fi[k] = field.kwargs[k]
            fields_d.append(fi)
        return self.connection.send_command(3, 0, {
            "bucket": self.name,
            "fields": fields_d,
        })
    def drop(self):
        return self.connection.send_command(5, 0, {
            'bucket': self.name,
        })
    def __iter__(self):
        from_table_id = None
        while True:
            table_ids = Command(self.connection, 14, 0, {
                'bucket': self.name,
                'from_table_id': from_table_id,
            }).execute()['items']
            if not table_ids:
                raise StopIteration

            for table_id in table_ids:
                from_table_id = table_id
                yield table_id
    def fields(self):
        result = []
        for field  in Command(self.connection, 8, 0, {
            'bucket': self.name,
        }).execute():
            result.append(Field(**field))
        return result


    def __repr__(self):
        return pretty(Command(self.connection, 8, 0, {
            'bucket': self.name,
        }).execute())
    def __str__(self):
        return repr(self)
        

class CrabException(BaseException):
    pass


class Constant:
    def __init__(self, data):
        self.data = data
    def __repr__(self):
        return repr(self.data)

class Expression:
    @classmethod
    def to_expr(cls, obj):
        if isinstance(obj, Constant):
            return obj.data
        if isinstance(obj, (list, tuple)):
            res = [Expression.to_expr(item) for item in obj]
        elif isinstance(obj, dict):
            res = {}
            for k in obj:
                res[k] = Expression.to_expr(obj[k])
        elif isinstance(obj, Expression):
            res = Expression.to_expr(obj.expr)
        elif isinstance(obj, Table):
            res = '%s[%d]' % (obj.bucket.name, obj.table_id)
        else:
            res = obj
        return res
    def __and__(self, *others):
        return Expression(['$and', self.expr] + list(others))
    def __or__(self, *others):
        return Expression(['$or', self.expr] + list(others))
    def __invert__(self):
        return Expression(['$not', self.expr])
    def __str__(self):
        return str(self.expr)
    def __repr__(self):
        if isinstance(self.expr, (str, unicode)):
            rep = repr(self.expr)
        else:
            rep = str(self)
        return 'Expression(%s)' % rep
    def __init__(self, expr):
        self.expr = expr
    def __add__(self, other):
        return Expression(['$add', self.expr, other])
    def __div__(self, other):
        return Expression(['$div', self.expr, other])
    def __eq__(self, other):
        return Expression(['$eq', self.expr, other])
    def __ge__(self, other):
        return Expression(['$gte', self.expr, other])
    def __gt__(self, other):
        return Expression(['$gt', self.expr, other])
    def __le__(self, other):
        return Expression(['$lte', self.expr, other])
    def __lshift__(self, other):
        return Expression(['$shl', self.expr, other])
    def __lt__(self, other):
        return Expression(['$lt', self.expr, other])
    def __mod__(self, other):
        return Expression(['$mod', self.expr, other])
    def __mul__(self, other):
        return Expression(['$mul', self.expr, other])
    def __ne__(self, other):
        return Expression(['$ne', self.expr, other])
    def __neg__(self):
        return Expression(['$neg', self.expr])
    def __pow__(self, other):
        return Expression(['$pow', self.expr, other])
    def __radd__(self, other):
        return Expression(['$add', other, self.expr])
    def __rdiv__(self, other):
        return Expression(['$div', other, self.expr])
    def __rmul__(self, other):
        return Expression(['$mul', other, self.expr])
    def __rshift__(self, other):
        return Expression(['$shr', self.expr, other])
    def __rsub__(self, other):
        return Expression(['$sub', other, self.expr])
    def __sub__(self, other):
        return Expression(['$sub', self.expr, other])
    def __xor__(self, other):
        return Expression(['$xor', self.expr, other])
    def in_(self, other):
        if isinstance(other, (list, tuple)):
            other = Constant(other)
        return Expression(['$in', self.expr, other])
    def __nonzero__(self):
        raise CrabException("Don't use an Expression object like this.")
    def __coerce__(self, y):
        return self, y
    def __getattr__(self, name):
        return lambda *args: Expression(['$' + name, self.expr] + list(args))

class Geo(Expression):
    def __init__(self, lat, lon):
        if isinstance(lat, (int, long, float)) and isinstance(lon, (int, long, float)):
            MAX = 2 ** 32
            lat = int((90 - lat) / 180.0 * MAX)
            if lat < 0: lat = 0
            if lat >= MAX: lat = MAX - 1
            lon = int((180 + lon) / 360.0 * MAX) % MAX
            geo = ((lon << 32) | lat)
            self.expr = geo
        else:
            self.expr = ['$geo_from_latlon', lat, lon]

class Field:
    def __init__(self, nbits=None, name=None, **kwargs):
        self.nbits = nbits
        self.name = name
        self.kwargs = kwargs
    def __repr__(self):
        s = ''
        if self.name:
            s += self.name
        s += ':%d' % self.nbits
        if self.kwargs.get('is_signed'):
            s += ' S'
        if self.kwargs.get('is_primary_key'):
            s += ' P'
        return '<%s>' % s

class Record:
    def __getattr__(self, name):
        return Expression(name)
    def __repr__(self):
        return '<Record object>'
    def __str__(self):
        return repr(self)


def pretty(obj, tab=0, printtab=1):
    space1 = space2 = '    ' * tab
    if not printtab:
        space1 = ''
    if isinstance(obj, (list, tuple)):
        return space1 + '[\n' + ',\n'.join([pretty(item, tab + 1) for item in obj]) + '\n' + space2 + ']'
    if isinstance(obj, dict):
        return space1 + '{\n' + ',\n'.join([pretty(k, tab + 1) + ': ' + pretty(v, tab + 1, 0) for k, v in obj.iteritems()]) + '\n' + space2 + '}'
    return space1 + repr(obj)
        
class Command:
    result = None
    def __init__(self, connection, command, flags, params):
        self.connection = connection
        self.command = command
        self.params = params
        self.flags = flags
    def __str__(self):
        return repr(self)
    def __repr__(self):
        return pretty(self.execute())
    def __getslice__(self, first, last):
        self.params['offset'] = self.params.get('offset', 0) + first
        if 'limit' in self.params:
            self.params['limit'] = min(self.params['limit'], last - first)
        else:
            self.params['limit'] = last - first
        return self
    def __getattr__(self, name):
        def func(val):
            self.result = None
            params = {name: val}
            params.update(self.params)
            return Command(self.connection, self.command, self.flags, params)
        return func
    def execute(self):
        return self.connection.send_command(self.command, self.flags, self.params)
    def __getitem__(self, name):
        if self.result == None:
            self.result = self.execute()
        if isinstance(name, (int, long)):
            return self.result['items'][name]
        else:
            return self.result[name]
    def __iter__(self):
        return iter(self['items'])
    def first(self):
        if self.result == None:
            self.params['limit'] = 1
        for r in self:
            return r
    def filter(self, *args):
        params = {'filter': ['$and'] + list(args)}
        params.update(self.params)
        return Command(self.connection, self.command, self.flags, params)
    def count(self):
        if self.result == None:
            self.params['limit'] = 0
        return self['num_items']

CRABDB_DEFAULT_HOST = 'localhost'
CRABDB_DEFAULT_PORT = 2222

class Connection:
    version = 1
    def __parse_host_port(self, host_and_port):
        matched = re.search(r'^(?P<host>[a-z0-9\.\-:\[\]]*?)(:(?P<port>[0-9]+))?$', host_and_port)
        if matched:
            return matched.group('host') or CRABDB_DEFAULT_HOST, matched.group('port') or CRABDB_DEFAULT_PORT
        else:
            raise CrabException('Invalid host and port: %s' % host_and_port)
    def __init__(self, host=CRABDB_DEFAULT_HOST, port=CRABDB_DEFAULT_PORT, retry=True, enable_gzip=False):
        self.host_port = host, port
        self.seq = 0
        self.sock = None
        self.retry = retry
        self.enable_gzip = enable_gzip
    def __reconnect(self):
        try:
            print 'Connecting to CrabDB server', self.host_port
            self.sock = socket.create_connection(self.host_port, 2)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 5)
            self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)
            self.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 3)
            self.sock.settimeout(None)
            return True
        except Exception, e:
            print 'CrabDB reconnect error:', e
            return False
    def __nonzero__(self):
        return True
    def __getattr__(self, name):
        return Bucket(self, name)
    def __getitem__(self, name):
        return Bucket(self, name)
    def send_command(self, command, flags=0, payload={}):
        payload = Expression.to_expr(payload)
        packed = msgpack.packb(payload)
        if self.enable_gzip:
            packed = zlib.compress(packed)
            flags |= REQUEST_FLAG_GZIP

        def robust_recv(sock, size):
            data = ''
            while size:
                #ready = select.select([sock], [], [sock], 600)
                if 1: # ready[0] or ready[2]:
                    piece = sock.recv(size)
                    if piece == '':
                        raise socket.error()
                    data += piece
                    size -= len(piece)
                else:
                    raise socket.error()
            return data
            
        while True:
            try:
                if self.sock == None:
                    raise socket.error()
                self.seq += 1
                to_send = struct.pack('<iiqiii', self.version, 0, self.seq, command, flags, len(packed)) + packed
                while to_send:
                    sendsize = self.sock.send(to_send)
                    to_send = to_send[sendsize:]
                if not flags & REQUEST_FLAG_NO_REPLY:
                    seq, status, size = struct.unpack('<qii', robust_recv(self.sock, 16))
                    if self.seq != seq:
                        print self.seq, seq
                        raise socket.error()
                    if size > 0:
                        response_packed = robust_recv(self.sock, size)
                        if self.enable_gzip:
                            response_packed = zlib.decompress(response_packed)
                        response = msgpack.unpackb(response_packed)
                    else:
                        response = None
                    if status == RESPONSE_STATUS_SUCCESS:
                        return response
                    elif status == RESPONSE_STATUS_REDIRECTION:
                        print 'CrabDB is redirecting to', (response['host'], response['port'])
                        if self.host_port == (response['host'], response['port']):
                            print 'ERROR: redirecting to itself.'
                            time.sleep(1)
                        else:
                            self.host_port = (response['host'], response['port'])
                        raise socket.error()
                    else:
                        raise CrabException('CrabDB Exception %d: %s' % (status, response))
                return
            except socket.error:
                if not self.__reconnect():
                    if not self.retry:
                        return
                    time.sleep(0.1)
        
    def __iter__(self):
        return iter(self.send_command(6))
    def __repr__(self):
        return 'CrabDB connection on %s:%d\n' % self.host_port + '\n'.join(self)
    def __str__(self):
        return repr(self)
    def stats(self):
        return self.send_command(10)
    def freeze(self, is_freeze):
        return self.send_command(11, payload={'freeze': is_freeze})
    def binlog_stats(self):
        return self.send_command(15)
        