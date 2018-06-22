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

        private async Task WriteStartCommandAsync(IRespWriter writer)
        {
            await writer.WriteStartArrayAsync();
            await writer.WriteBulkStringAsync(_name);
        }

        protected virtual Task WriteParametersAsync(IRespWriter writer)
        {
            return Task.FromResult(0);
        }

        private async Task WriteEndCommandAsync(IRespWriter writer)
        {
            await writer.WriteEndArrayAsync();
        }

        protected abstract CommandResult<T> ReadResult(IRespReader reader);
    }
}
