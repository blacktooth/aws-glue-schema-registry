// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AWSGsrSerDe.common;
using Confluent.Kafka;

namespace AWSGsrSerDe.deserializer
{
    /// <summary>
    /// Glue Schema Registry Kafka Generic Deserializer responsible for de-serializing
    /// </summary>
    public class GlueSchemaRegistryKafkaDeserializer: IDeserializer<object>, IAsyncDeserializer<object>
    {
        private readonly DataFormatDeserializerFactory _dataFormatDeserializerFactory =
            DataFormatDeserializerFactory.GetInstance();

        private readonly GlueSchemaRegistryDeserializer _glueSchemaRegistryDeserializer;

        private readonly SecondaryDeserializer _secondaryDeserializer = SecondaryDeserializer.Build();

        private GlueSchemaRegistryConfiguration _configuration;


        /// <summary>
        /// Initializes a new instance of the <see cref="GlueSchemaRegistryKafkaDeserializer"/> class.
        /// </summary>
        /// <param name="configs">configuration elements for de-serializer</param>
        public GlueSchemaRegistryKafkaDeserializer(Dictionary<string, dynamic> configs)
        {
            Configure(configs);
            _glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializer();
        }

        /// <summary>
        /// Configures the <see cref="GlueSchemaRegistryKafkaDeserializer"/> instance
        /// </summary>
        /// <param name="configs">configuration elements for de-serializer</param>
        public void Configure(Dictionary<string, dynamic> configs)
        {
            _configuration = new GlueSchemaRegistryConfiguration(configs);
            InitSecondaryDeserializerIfProvided(configs);
        }

        /// <inheritdoc />
        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (ShouldUseSecondaryDeserializer(data))
            {
                return _secondaryDeserializer.Deserialize(data, isNull, context);
            }

            return Deserialize(context.Topic, data.ToArray());
        }

        /// <inheritdoc />
        public Task<object> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            var dataSpan = new ReadOnlySpan<byte>(data.ToArray());
            if (ShouldUseSecondaryDeserializer(dataSpan))
            {
                return _secondaryDeserializer.DeserializeAsync(data, isNull, context);
            }

            var task = Task<object>.Factory.StartNew(() => Deserialize(context.Topic, data.ToArray()));
            return task;
        }

        /// <summary>
        /// De-serialize operation for de-serializing the byte array to an Object.
        /// </summary>
        /// <param name="topic">Kafka topic name</param>
        /// <param name="data">serialized data to be de-serialized in byte array</param>
        /// <returns>de-serialized object instance</returns>
        public object Deserialize(string topic, byte[] data)
        {
            if (data == null)
            {
                return null;
            }

            if (!_glueSchemaRegistryDeserializer.CanDecode(data))
            {
                throw new AwsSchemaRegistryException("Byte data cannot be decoded");
            }

            var decodedBytes = _glueSchemaRegistryDeserializer.Decode(data);
            var schemaRegistrySchema = _glueSchemaRegistryDeserializer.DecodeSchema(data);

            var dataFormat = schemaRegistrySchema.DataFormat;
            var deserializer = _dataFormatDeserializerFactory.GetDeserializer(dataFormat, _configuration);

            var result = deserializer.Deserialize(decodedBytes, schemaRegistrySchema);

            return result;
        }

        private bool ShouldUseSecondaryDeserializer(ReadOnlySpan<byte> data)
        {
            var gsrHeader = new ReadOnlySpan<byte>(new[] { GlueSchemaRegistryConstants.HeaderVersionByte });
            return !data.StartsWith(gsrHeader);
        }

        private void InitSecondaryDeserializerIfProvided(Dictionary<string, dynamic> configs)
        {
            if (!configs.ContainsKey(GlueSchemaRegistryConstants.SecondaryDeserializer))
            {
                return;
            }

            if (!_secondaryDeserializer.ValidateAndInit(configs))
            {
                throw new AwsSchemaRegistryException("The secondary deserializer is not from Kafka");
            }
        }
    }
}
