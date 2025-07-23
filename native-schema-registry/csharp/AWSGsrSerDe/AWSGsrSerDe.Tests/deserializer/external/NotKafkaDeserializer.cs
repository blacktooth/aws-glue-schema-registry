using System;
using Confluent.Kafka;

namespace AWSGsrSerDe.Tests.deserializer.external
{
    public class NotKafkaDeserializer
    {
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return data.ToArray();
        }
    }
}
