using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis
{
    internal abstract class Command<T>
    {
        readonly Stream _stream;
        readonly string _name;

        protected Command(Stream stream, string name)
        {
            _stream = stream;
            _name = name;
        }

        public T Execute()
        {
            using (var writer = new RespWriter(_stream))
            {
                WriteStartCommand(writer);
                WriteParameters(writer);
                WriteEndCommand(writer);
            }

            using (var reader = new RespReader(_stream))
            {
                return ReadResult(reader).Result;
            }
        }

        public async Task<T> ExecuteAsync()
        {
            using (var writer = new RespWriter(_stream))
            {
                await WriteStartCommandAsync(writer);
                await WriteParametersAsync(writer);
                await WriteEndCommandAsync(writer);
            }

            using (var reader = new RespReader(_stream))
            {
                return ReadResult(reader).Result;
            }
        }

        private void WriteStartCommand(IRespWriter writer)
        {
            writer.WriteStartArray();
            writer.WriteBulkString(_name);
        }

        private async Task WriteStartCommandAsync(IAsyncRespWriter writer)
        {
            await writer.WriteStartArrayAsync();
            await writer.WriteBulkStringAsync(_name);
        }

        protected virtual void WriteParameters(IRespWriter writer)
        {
        }

        protected virtual Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            return Task.FromResult(0);
        }

        private void WriteEndCommand(IRespWriter writer)
        {
            writer.WriteEndArray();
        }

        private async Task WriteEndCommandAsync(IAsyncRespWriter writer)
        {
            await writer.WriteEndArrayAsync();
        }

        protected abstract CommandResult<T> ReadResult(IRespReader reader);
    }
}
