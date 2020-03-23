using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Hashes
{
    internal sealed class HIncrBy : IntegerValueCommand
    {
        private readonly byte[] _key;
        private readonly byte[] _field;
        private readonly long _value;

        public HIncrBy(Stream stream, byte[] key, byte[] field, long value)
            : base(stream, "HINCRBY")
        {
            _key = key;
            _field = field;
            _value = value;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_field);
            writer.WriteInteger(_value);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_field);
            await writer.WriteIntegerAsync(_value);
        }
    }
}
