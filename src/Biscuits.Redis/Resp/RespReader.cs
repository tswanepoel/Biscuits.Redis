using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Biscuits.Redis.Resp
{
    public class RespReader : IRespReader, IAsyncRespReader, IDisposable
    {
        private readonly Stream _stream;
        private byte[] _singleByteBuffer;
        private long _currentLength = 1;
        private bool _disposed;
        
        public RespReader(Stream stream)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public ReadState ReadState { get; private set; } = ReadState.Initial;

        public RespDataType ReadDataType()
        {
            ValidateNotDisposed();

            if (ReadState != ReadState.Initial && ReadState != ReadState.DataType)
            {
                throw new InvalidOperationException();
            }

            int ch = ReadByte();

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

        public async Task<RespDataType> ReadDataTypeAsync()
        {
            ValidateNotDisposed();

            if (ReadState != ReadState.Initial && ReadState != ReadState.DataType)
            {
                throw new InvalidOperationException();
            }

            int ch = await ReadByteAsync();

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

        public async Task<(bool success, long length)> TryReadStartArrayAsync()
        {
            ValidateNotDisposed();

            if (ReadState != ReadState.Array)
            {
                throw new InvalidOperationException();
            }

            string countString = await ReadSimpleStringValueCoreAsync();
            long length = long.Parse(countString, CultureInfo.InvariantCulture);
            
            if (length == -1)
            {
                _currentLength--;

                ReadState = ReadState.DataType;
                return (false, length);
            }

            _currentLength = length;
            ReadState = ReadState.DataType;
            return (true, length);
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

        public Task ReadEndArrayAsync()
        {
            ReadEndArray();
            return Task.CompletedTask;
        }

        public string ReadSimpleStringValue()
        {
            ValidateNotDisposed();

            string value = ReadSimpleStringValueCore();
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public async Task<string> ReadSimpleStringValueAsync()
        {
            ValidateNotDisposed();

            string value = await ReadSimpleStringValueCoreAsync();
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public string ReadErrorValue()
        {
            ValidateNotDisposed();

            string value = ReadSimpleStringValueCore();
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public async Task<string> ReadErrorValueAsync()
        {
            ValidateNotDisposed();

            string value = await ReadSimpleStringValueCoreAsync();
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public byte[] ReadBulkStringValue()
        {
            ValidateNotDisposed();

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

            byte[] bytes = ReadBytes((int)length);
            ReadByte();
            ReadByte();
            _currentLength--;

            ReadState = ReadState.DataType;
            return bytes;
        }

        public async Task<byte[]> ReadBulkStringValueAsync()
        {
            ValidateNotDisposed();

            string lengthString = await ReadSimpleStringValueCoreAsync();
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
            
            byte[] bytes = await ReadBytesAsync((int)length);
            await ReadByteAsync();
            await ReadByteAsync();
            _currentLength--;

            ReadState = ReadState.DataType;
            return bytes;
        }

        public long ReadIntegerValue()
        {
            ValidateNotDisposed();

            string valueString = ReadSimpleStringValueCore();
            long value = long.Parse(valueString, CultureInfo.InvariantCulture);
            _currentLength--;

            ReadState = ReadState.DataType;
            return value;
        }

        public async Task<long> ReadIntegerValueAsync()
        {
            ValidateNotDisposed();

            string valueString = await ReadSimpleStringValueCoreAsync();
            long value = long.Parse(valueString, CultureInfo.InvariantCulture);
            _currentLength--;
            
            ReadState = ReadState.DataType;
            return value;
        }

        public void Close()
        {
            Dispose(true);
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
                    _stream.Close();
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
            int ch;

            while ((ch = ReadByte()) > 0 && ch != '\r')
            {
                sb.Append(ch);
            }

            ReadByte();
            return sb.ToString();
        }

        private async Task<string> ReadSimpleStringValueCoreAsync()
        {
            var sb = new StringBuilder();
            int ch;
            
            while ((ch = await ReadByteAsync()) > 0 && ch != '\r')
            {
                sb.Append(ch);
            }

            await ReadByteAsync();
            return sb.ToString();
        }

        private int ReadByte()
        {
            return _stream.ReadByte();
        }

        private async Task<int> ReadByteAsync()
        {
            if (_singleByteBuffer == null)
            {
                _singleByteBuffer = new byte[1];
            }

            if (await _stream.ReadAsync(_singleByteBuffer, 0, 1) == 0)
            {
                throw new EndOfStreamException();
            }

            return _singleByteBuffer[0];
        }

        private byte[] ReadBytes(int count)
        {
            var result = new byte[count];
            int numRead = 0;

            do
            {
                int n = _stream.Read(result, numRead, count);
                
                if (n == 0)
                {
                    break;
                }

                numRead += n;
                count -= n;
            }
            while (count > 0);

            if (numRead != result.Length)
            {
                var trimmed = new byte[numRead];
                Buffer.BlockCopy(result, 0, trimmed, 0, numRead);

                return trimmed;
            }

            return result;
        }

        private async Task<byte[]> ReadBytesAsync(int count)
        {
            var result = new byte[count];
            int numRead = 0;

            do
            {
                int n = await _stream.ReadAsync(result, numRead, count);

                if (n == 0)
                {
                    break;
                }

                numRead += n;
                count -= n;
            }
            while (count > 0);

            if (numRead != result.Length)
            {
                var trimmed = new byte[numRead];
                Buffer.BlockCopy(result, 0, trimmed, 0, numRead);

                return trimmed;
            }

            return result;
        }
    }
}
