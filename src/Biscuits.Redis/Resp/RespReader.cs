using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace Biscuits.Redis.Resp
{
    public class RespReader : IRespReader, IDisposable
    {
        private readonly Encoding _encoding = new UTF8Encoding(false);
        //private readonly Stack<long> _ancestorCounts = new Stack<long>();
        private readonly BinaryReader _reader;
        private long _currentLength = 1;
        private bool _disposed;
        
        public RespReader(Stream stream)
        {
            _reader = new BinaryReader(stream ?? throw new ArgumentNullException(nameof(stream)), _encoding, true);
        }

        public ReadState ReadState { get; private set; } = ReadState.Initial;

        public RespDataType ReadDataType()
        {
            ValidateNotDisposed();

            if (ReadState != ReadState.Initial && ReadState != ReadState.DataType)
            {
                throw new InvalidOperationException();
            }

            char ch = _reader.ReadChar();

            switch (ch)
            {
                case '*':
                    ReadState = ReadState.Array;
                    return RespDataType.Array;

                case '+':
                    ReadState = ReadState.Value;
                    return RespDataType.SimpleString;

                case '-':
                    ReadState = ReadState.Value;
                    return RespDataType.Error;

                case ':':
                    ReadState = ReadState.Value;
                    return RespDataType.Integer;

                case '$':
                    ReadState = ReadState.Value;
                    return RespDataType.BulkString;

                default:
                    throw new InvalidDataException();
            }
        }

        public bool TryReadStartArray(out long length)
        {
            ValidateNotDisposed();

            if (ReadState != ReadState.Array)
            {
                throw new InvalidOperationException();
            }

            string countString = ReadSimpleStringValueCore();
            length = long.Parse(countString, CultureInfo.InvariantCulture);

            if (length == -1)
            {
                _currentLength--;

                ReadState = ReadState.DataType;
                return false;
            }
            
            _currentLength = length;
            ReadState = ReadState.DataType;
            return true;
        }

        public void ReadEndArray()
        {
            ValidateNotDisposed();

            if (ReadState != ReadState.Value)
            {
                throw new InvalidOperationException();
            }

            if (_currentLength > 0)
            {
                throw new InvalidOperationException();
            }

            _currentLength--;
            ReadState = ReadState.DataType;
        }

        public string ReadSimpleStringValue()
        {
            ValidateNotDisposed();

            if (ReadState == ReadState.Closed)
            {
                throw new InvalidOperationException();
            }

            string value = ReadSimpleStringValueCore();
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public string ReadErrorValue()
        {
            ValidateNotDisposed();

            if (ReadState == ReadState.Closed)
            {
                throw new InvalidOperationException();
            }

            string value = ReadSimpleStringValueCore();
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public byte[] ReadBulkStringValue()
        {
            ValidateNotDisposed();

            if (ReadState == ReadState.Closed)
            {
                throw new InvalidOperationException();
            }

            string lengthString = ReadSimpleStringValueCore();
            long length = long.Parse(lengthString, CultureInfo.InvariantCulture);
            
            if (length == -1)
            {
                ReadState = ReadState.DataType;
                return null;
            }

            if (length > int.MaxValue)
            {
                throw new NotSupportedException();
            }

            byte[] bytes = _reader.ReadBytes((int)length);
            _reader.ReadChar();
            _reader.ReadChar();
            _currentLength--;

            ReadState = ReadState.DataType;
            return bytes;
        }

        public long ReadIntegerValue()
        {
            ValidateNotDisposed();

            if (ReadState == ReadState.Closed)
            {
                throw new InvalidOperationException();
            }

            string valueString = ReadSimpleStringValueCore();
            long value = long.Parse(valueString, CultureInfo.InvariantCulture);
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public void Close()
        {
            ReadState = ReadState.Closed;
        }
        
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _reader.Dispose();
                }

                _disposed = true;
            }
        }

        private void ValidateNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(RespReader));
            }
        }

        private string ReadSimpleStringValueCore()
        {
            var sb = new StringBuilder();
            char ch;

            while ((ch = _reader.ReadChar()) > 0 && ch != '\r')
            {
                sb.Append(ch);
            }

            _reader.ReadChar();
            return sb.ToString();
        }
    }
}
