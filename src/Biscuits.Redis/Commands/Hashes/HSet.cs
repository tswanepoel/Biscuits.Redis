using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Hashes
{
    internal sealed class HSet : IntegerValueCommand
    {
        private readonly byte[] _key;
        private readonly byte[] _field;
        private readonly byte[] _value;

        public HSet(Stream stream, byte[] key, byte[] field, byte[] value)
            : base(stream, "HSET")
        {
            _key = key;
            _field = field;
            _value = value;
        }
        
        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_field);
            writer.WriteBulkString(_value);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_field);
            await writer.WriteBulkStringAsync(_value);
        }
    }
}
