using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using System;
using System.IO;

namespace Biscuits.Collections.Redis
{
    internal static class BsonConvert
    {
        public static byte[] SerializeObject(object value)
        {
            using var stream = new MemoryStream();
            using var writer = new BsonDataWriter(stream);

            new JsonSerializer().Serialize(writer, value);
            return stream.ToArray();
        }

        public static byte[] SerializeObject(object value, Type objectType)
        {
            using var stream = new MemoryStream();
            using var writer = new BsonDataWriter(stream);

            new JsonSerializer().Serialize(writer, value, objectType);
            return stream.ToArray();
        }

        public static object DeserializeObject(byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var reader = new BsonDataReader(stream);

            return new JsonSerializer().Deserialize(reader);
        }

        public static T DeserializeObject<T>(byte[] value)
        {
            using var stream = new MemoryStream(value);
            using var reader = new BsonDataReader(stream);

            return new JsonSerializer().Deserialize<T>(reader);
        }
    }
}
