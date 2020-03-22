using Biscuits.Redis.Resp;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace Biscuits.Redis.Commands.Lists
{
    internal sealed class LRange : ArrayOfBulkStringValueCommand
    {
        private readonly byte[] _key;
        private readonly long _start;
        private readonly long _stop;

        public LRange(Stream stream, byte[] key, long start, long stop)
            : base(stream, "LRANGE")
        {
            _key = key;
            _start = start;
            _stop = stop;
        }

        protected override void WriteParameters(IRespWriter writer)
        {
            writer.WriteBulkString(_key);
            writer.WriteBulkString(_start.ToString(CultureInfo.InvariantCulture));
            writer.WriteBulkString(_stop.ToString(CultureInfo.InvariantCulture));
        }

        protected override async Task WriteParametersAsync(IAsyncRespWriter writer)
        {
            await writer.WriteBulkStringAsync(_key);
            await writer.WriteBulkStringAsync(_start.ToString(CultureInfo.InvariantCulture));
            await writer.WriteBulkStringAsync(_stop.ToString(CultureInfo.InvariantCulture));
        }
    }
}
