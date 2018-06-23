using Biscuits.Redis.Resp;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands
{
    internal sealed class HExists : IntegerValueCommand
    {
        readonly byte[] _key;
        readonly byte[] _field;

        public HExists(Stream stream, byte[] key, byte[] field)
            : base(stream, "HEXISTS")
        {
            _key = key;
            _field = field;
        }
        
        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_field);
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_field);
        }
    }
}
