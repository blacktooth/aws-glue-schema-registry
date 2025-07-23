using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace AWSGsrSerDe.Tests.deserializer.external
{
    public class AsyncDeserializer : IAsyncDeserializer<object>
    {
        public Task<object> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            return Task.FromResult((object)data.ToArray());
        }
    }
}
