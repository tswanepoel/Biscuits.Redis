using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Biscuits.Redis.Resp
{
    public class RespWriter : IRespWriter, IDisposable
    {
        private const int DefaultBufferSize = 1024/*1KB*/;
        private readonly Encoding _encoding = new UTF8Encoding(false);
        private readonly Stack<StreamWriter> _ancestors = new Stack<StreamWriter>();
        private readonly Stack<long> _ancestorLengths = new Stack<long>();
        private StreamWriter _current;
        private long _currentLength;
        private bool _disposed;

        public RespWriter(Stream stream)
        {
            _current = new StreamWriter(stream ?? throw new ArgumentNullException(nameof(stream)), _encoding, DefaultBufferSize, true);
        }

        public WriteState WriteState { get; private set; } = WriteState.Start;

        public async Task WriteStartArrayAsync()
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            await _current.WriteAsync('*');

            _ancestors.Push(_current);
            _ancestorLengths.Push(_currentLength);

            _current = new StreamWriter(new MemoryStream(), _encoding);
            _currentLength = 0L;

            WriteState = WriteState.Array;
        }

        public async Task WriteEndArrayAsync()
        {
            ValidateNotDisposed();

            if (WriteState != WriteState.Array)
            {
                throw new InvalidOperationException();
            }

            StreamWriter parent = _ancestors.Pop();
            long parentCount = _ancestorLengths.Pop();

            await parent.WriteAsync(_currentLength.ToString(CultureInfo.InvariantCulture));
            await parent.WriteAsync("\r\n");
            await parent.FlushAsync();

            await _current.FlushAsync();
            _current.BaseStream.Seek(0, SeekOrigin.Begin);
            await _current.BaseStream.CopyToAsync(parent.BaseStream);
            _current.Dispose();

            _current = parent;
            _currentLength = parentCount + 1;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task WriteNullArrayAsync()
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            await _current.WriteAsync("*-1\r\n");
            _currentLength++;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task WriteSimpleStringUnsafeAsync(string value)
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            await _current.WriteAsync('+');
            await _current.WriteAsync(value);
            await _current.WriteAsync("\r\n");

            _currentLength++;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task WriteErrorUnsafeAsync(string value)
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            await _current.WriteAsync('-');
            await _current.WriteAsync(value);
            await _current.WriteAsync("\r\n");

            _currentLength++;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task WriteIntegerAsync(long value)
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            await _current.WriteAsync(':');
            await _current.WriteAsync(value.ToString(CultureInfo.InvariantCulture));
            await _current.WriteAsync("\r\n");

            _currentLength++;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task WriteBulkStringAsync(string value)
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            await _current.WriteAsync('$');

            if (value == null)
            {
                await _current.WriteAsync("-1\r\n");
                return;
            }

            await _current.WriteAsync(value.Length.ToString(CultureInfo.InvariantCulture));
            await _current.WriteAsync("\r\n");

            await _current.WriteAsync(value);
            await _current.WriteAsync("\r\n");

            _currentLength++;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task WriteBulkStringAsync(byte[] bytes)
        {
            ValidateNotDisposed();

            if (WriteState == WriteState.Closed)
            {
                throw new InvalidOperationException();
            }

            await _current.WriteAsync('$');

            if (bytes == null)
            {
                await _current.WriteAsync("-1\r\n");
            }
            else
            {
                await _current.WriteAsync(bytes.LongLength.ToString(CultureInfo.InvariantCulture));
                await _current.WriteAsync("\r\n");
                await _current.FlushAsync();

                using (var writer = new BinaryWriter(_current.BaseStream, _encoding, true))
                {
                    writer.Write(bytes);
                }

                await _current.WriteAsync("\r\n");
            }

            _currentLength++;

            if (_ancestors.Count == 0)
            {
                WriteState = WriteState.Start;
            }
        }

        public async Task FlushAsync()
        {
            ValidateNotDisposed();
            await _current.FlushAsync();
        }

        public void Close()
        {
            WriteState = WriteState.Closed;
        }
        
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _current.Flush();

                while (_ancestors.Count != 0)
                {
                    _current.Dispose();
                    _current = _ancestors.Pop();
                }
            }

            _disposed = true;
        }

        private void ValidateNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RespWriter));
            }
        }
    }
}
