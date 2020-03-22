using Biscuits.Redis.Resp;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Hashes
{
    internal sealed class HMSet : SimpleStringValueCommand
    {
        private readonly byte[] _key;
        private readonly IEnumerable<byte[]> _fieldsAndValues;

        public HMSet(Stream stream, byte[] key, IEnumerable<byte[]> fieldsAndValues)
            : base(stream, "HSET")
        {
            _key = key;
            _fieldsAndValues = fieldsAndValues;
        }
        
        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);

            foreach (var fieldOrValue in _fieldsAndValues)
            {
                writer.WriteBulkString(fieldOrValue);
            }
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);

            foreach (var fieldOrValue in _fieldsAndValues)
            {
                await writer.WriteBulkStringAsync(fieldOrValue);
            }
        }
    }
}
