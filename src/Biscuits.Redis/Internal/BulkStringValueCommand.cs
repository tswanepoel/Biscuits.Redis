using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    internal abstract class BulkStringValueCommand
    {
        private readonly Stream _stream;
        private readonly string _name;

        protected BulkStringValueCommand(Stream stream, string name)
        {
            _stream = stream;
            _name = name;
        }

        public async Task<CommandResult<byte[]>> ExecuteAsync()
        {
            using (var writer = new RespWriter(_stream))
            {
                await WriteStartCommandAsync(writer);
                await WriteParametersAsync(writer);
                await WriteEndCommandAsync(writer);
            }

            using (var reader = new RespReader(_stream))
            {
                return ReadResult(reader);
            }
        }

        protected virtual async Task WriteStartCommandAsync(IRespWriter writer)
        {
            await writer.WriteStartArrayAsync();
            await writer.WriteBulkStringAsync(_name);
        }

        protected virtual Task WriteParametersAsync(IRespWriter writer)
        {
            return Task.FromResult(0);
        }

        protected virtual async Task WriteEndCommandAsync(IRespWriter writer)
        {
            await writer.WriteEndArrayAsync();
        }

        private CommandResult<byte[]> ReadResult(IRespReader reader)
        {
            RespDataType dataType = reader.ReadDataType();

            if (dataType == RespDataType.Error)
            {
                string err = reader.ReadErrorValue();
                return CommandResult<byte[]>.Error(err);
            }

            if (dataType != RespDataType.BulkString)
            {
                throw new InvalidDataException();
            }

            byte[] result = reader.ReadBulkStringValue();
            return CommandResult.Success(result);
        }
    }
}
