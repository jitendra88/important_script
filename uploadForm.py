import urllib
import uuid


class UploadForm:
    def __init__(self, boundary=None):
        self.set_boundary(boundary)
        self.fields = []

    def set_boundary(self, boundary):
        self.boundary = urllib.quote(boundary) if boundary else '__--__{%s}__--__' % uuid.uuid4()

    def add_field(self, name, value, **params):
        '''see _Field.__init__() for the description of the parameters.'''
        field = _Field(name, value, **params)
        self.fields.append(field)
        return field

    def get_request_headers(self, calculate_content_length=True):
        assert self.fields
        headers = {'Content-Type': 'multipart/form-data; boundary=' + self.boundary}
        if not calculate_content_length:
            return headers
        content_length = self.get_size()
        headers['Content-Length'] = str(content_length)
        return (headers, content_length)

    def __iter__(self):
        assert self.fields
        boundary = '--' + self.boundary + '\r\n'
        for field in self.fields:
            yield boundary
            for chunk in field:
                yield chunk
        yield '--'
        yield self.boundary
        yield '--\r\n'

    def __str__(self):
        return ''.join(s for s in self)

    def dump(self, f):
        for chunk in self:
            f.write(chunk)

    def get_size(self):
        return (2 + len(self.boundary) + 2) * (len(self.fields) + 1) + 2 + sum(
            field.get_size() for field in self.fields)

class _Header:
    def __init__(self, _header_name, _header_value, **params):
        self.name = _header_name
        self.value = _header_value
        self.params = params

    def __iter__(self):
        yield _stringify(self.name)
        yield ': '
        yield _stringify(self.value)
        for name, value in self.params.iteritems():
            yield '; '
            yield _stringify(name)
            yield '="'
            yield _stringify(value).replace('\\', '\\\\').replace('"', '\\"')
            yield '"'
        yield '\r\n'

    def get_size(self):
        def _param_size(name, value):
            value = _stringify(value)
            return 5 + len(_stringify(name)) + len(value) + value.count('\\') + value.count('"')

        return 4 + len(_stringify(self.name)) + len(_stringify(self.value)) + sum(
            _param_size(name, value) for name, value in self.params.iteritems())


class _Field:
    BUFSIZE = 16 * 1024  # BUFSIZE used when yielding data from file objects

    def __init__(self, name, value, **params):
        '''
        @param params: Contains additional key-value pairs besides the "name" parameter for the Content-Disposition header. Example: filename='x.zip'
        @param value: This parameter can be a string, a unicode object, or a (file-object, size) tuple. In case of unicode object the string
                is converted to utf-8 before converting to mime. In case of a file-object we read the data starting from the current file pointer.
        '''
        params['name'] = name
        self.headers = [_Header('Content-Disposition', 'form-data', **params)]
        assert isinstance(value, (str, unicode, tuple))
        self.value = value

    def add_header(self, name, value, **params):
        '''	You can add additional mime headers, for example a "Content-Type" header with "text/plain" value with optional parameters like charset="UTF-8"'''
        self.headers.append(_Header(name, value, **params))

    def __iter__(self):
        for header in self.headers:
            for chunk in header:
                yield chunk
        yield '\r\n'

        if isinstance(self.value, str):
            yield self.value
        elif isinstance(self.value, unicode):
            yield self.value.encode('utf-8')
        else:
            bytes_left = self.value[1]
            while bytes_left > 0:
                bytes_to_read = min(self.BUFSIZE, bytes_left)
                data = self.value[0].read(bytes_to_read)
                if not data:
                    raise Exception('The specified file object doesn\'t contain enough data!')
                yield data
                bytes_left -= len(data)
        yield '\r\n'

    def __str__(self):
        return ''.join(s for s in self)

    def get_size(self):
        size = sum(header.get_size() for header in self.headers) + 2

        if isinstance(self.value, str):
            size += len(self.value)
        elif isinstance(self.value, unicode):
            size += len(self.value.encode('utf-8'))
        else:
            size += self.value[1]

        size += 2  # newline
        return size


def _stringify(s):
    return s.encode('utf-8') if isinstance(s, unicode) else s
