using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class HSetNX : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly byte[] _field;
        readonly byte[] _value;

        public HSetNX(Stream stream, byte[] key, byte[] field, byte[] value)
            : base(stream, "HSETNX")
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
