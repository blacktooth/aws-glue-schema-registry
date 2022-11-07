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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using AWSGsrSerDe.common;
using AWSGsrSerDe.deserializer;
using Confluent.Kafka;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.deserializer
{
    [TestFixture]
    public class SecondaryDeserializerTest
    {
        private static readonly Dictionary<string, dynamic> EmptyConfig = new Dictionary<string, dynamic>();

        private static readonly GlueSchemaRegistryKafkaDeserializer KafkaDeserializer =
            new GlueSchemaRegistryKafkaDeserializer(EmptyConfig);
        
        [Test]
        public void Test_InitAndValidate_Throws_Exception_For_Invalid_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.NotKafkaDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };
            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => new GlueSchemaRegistryKafkaDeserializer(configs));
            Assert.AreEqual("The secondary deserializer is not from Kafka", exception.Message);
        }

        [Test]
        public void Test_InitAndValidate_Throws_Exception_For_Empty_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    ""
                },
            };
            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => new GlueSchemaRegistryKafkaDeserializer(configs));
            Assert.AreEqual("Can't find the class or instantiate it.", exception.Message);
        }

        [Test]
        public void Test_InitAndValidate_Throws_Exception_For_Null_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    null
                },
            };
            var exception = Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => new GlueSchemaRegistryKafkaDeserializer(configs));
            Assert.AreEqual("Invalid secondary de-serializer configuration.", exception.Message);
        }

        [Test]
        public void Test_InitAndValidate_Succeed_For_Valid_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.ThirdPartyDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            var secondaryDeserializer = SecondaryDeserializer.Build();
            Assert.IsTrue(secondaryDeserializer.ValidateAndInit(configs));
        }

        [Test]
        public void Test_InitAndValidate_Succeed_For_Valid_Async_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.AsyncDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            var secondaryDeserializer = SecondaryDeserializer.Build();
            Assert.IsTrue(secondaryDeserializer.ValidateAndInit(configs));
        }

        [Test]
        public void Test_Deserialize_Throws_Exception_For_Secondary_Deserializer_Null_obj_Field()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.ThirdPartyDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            var secondaryDeserializer = SecondaryDeserializer.Build();
            secondaryDeserializer.ValidateAndInit(configs);

            secondaryDeserializer.GetType()
                .GetField("obj", BindingFlags.NonPublic | BindingFlags.Instance)
                ?.SetValue(secondaryDeserializer, null);

            var data = new[] { (byte)0, (byte)1 };
            Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => { secondaryDeserializer.Deserialize(data, false, SerializationContext.Empty); });
        }

        [Test]
        public void Test_Deserialize_Succeed_For_Valid_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.ThirdPartyDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            var secondaryDeserializer = SecondaryDeserializer.Build();
            secondaryDeserializer.ValidateAndInit(configs);
            var data = new ReadOnlySpan<byte>(new[] { (byte)0, (byte)1 });
            var deserializedData = secondaryDeserializer.Deserialize(data, false, SerializationContext.Empty);
            Assert.IsTrue(deserializedData is byte[]);
            var deserializedBytes = (byte[])deserializedData;
            Assert.AreEqual(data.ToArray(), deserializedBytes.ToArray());
        }

        [Test]
        public void Test_Deserialize_Throws_Exception_For_Async_Secondary_Deserializer_Null_obj_Field()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.AsyncDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            var secondaryDeserializer = SecondaryDeserializer.Build();
            Assert.IsTrue(secondaryDeserializer.ValidateAndInit(configs));
            secondaryDeserializer.GetType()
                .GetField("obj", BindingFlags.NonPublic | BindingFlags.Instance)
                ?.SetValue(secondaryDeserializer, null);
            var readOnlyMemory = new ReadOnlyMemory<byte>(new[] { (byte)0, (byte)1 });

            Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => secondaryDeserializer.DeserializeAsync(readOnlyMemory, false, SerializationContext.Empty));
        }

        [Test]
        public void Test_Deserialize_Succeed_For_Valid_Async_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.AsyncDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            var secondaryDeserializer = SecondaryDeserializer.Build();
            secondaryDeserializer.ValidateAndInit(configs);
            var data = new ReadOnlyMemory<byte>(new[] { (byte)0, (byte)1 });
            var deserializedData = secondaryDeserializer.DeserializeAsync(data, false, SerializationContext.Empty);
            Assert.IsTrue(deserializedData.Result is byte[]);
            var deserializedBytes = (byte[])deserializedData.Result;
            Assert.AreEqual(data.ToArray(), deserializedBytes.ToArray());
        }
        
        [Test]
        public void Test_GSR_Deserialize_Succeed_With_Valid_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.ThirdPartyDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };
            KafkaDeserializer.Configure(configs);
            var data = new ReadOnlySpan<byte>(new[] { (byte)0, (byte)1 });
            var deserializedData = KafkaDeserializer.Deserialize(data, false, SerializationContext.Empty);
            Assert.IsTrue(deserializedData is byte[]);
            var deserializedBytes = (byte[])deserializedData;
            Assert.AreEqual(data.ToArray(), deserializedBytes.ToArray());
        }
        
        [Test]
        public void Test_GSR_Deserialize_Succeed_With_Valid_Async_Secondary_Deserializer()
        {
            var configs = new Dictionary<string, dynamic>
            {
                {
                    GlueSchemaRegistryConstants.SecondaryDeserializer,
                    "AWSGsrSerDe.Tests.deserializer.external.AsyncDeserializer, " +
                    "AWSGsrSerDe.Tests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
                },
            };

            KafkaDeserializer.Configure(configs);
            var data = new ReadOnlyMemory<byte>(new[] { (byte)0, (byte)1 });
            var deserializedData = KafkaDeserializer.DeserializeAsync(data, false, SerializationContext.Empty);
            Assert.IsTrue(deserializedData.Result is byte[]);
            var deserializedBytes = (byte[])deserializedData.Result;
            Assert.AreEqual(data.ToArray(), deserializedBytes.ToArray());
        }
    }
}
