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
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using AWSGsrSerDe.common;
using Confluent.Kafka;
using Type = System.Type;

namespace AWSGsrSerDe.deserializer
{
    public class SecondaryDeserializer
    {
        private Type clz;
        private object obj;

        private SecondaryDeserializer()
        {
        }

        public static SecondaryDeserializer Build()
        {
            return new SecondaryDeserializer();
        }

        public bool ValidateAndInit(Dictionary<string, object> configs)
        {
            var className = configs[GlueSchemaRegistryConstants.SecondaryDeserializer];
            if (className is null)
            {
                throw new AwsSchemaRegistryException("Invalid secondary de-serializer configuration.");
            }

            var secondaryDeserializerTypeName = className.ToString();

            try
            {
                clz = Type.GetType(secondaryDeserializerTypeName!);
                obj = Activator.CreateInstance(clz!);
                var count = clz
                    .GetInterfaces().Count(classInterface =>
                        classInterface.FullName != null &&
                        (classInterface.FullName.StartsWith("Confluent.Kafka.IDeserializer") ||
                         classInterface.FullName.StartsWith("Confluent.Kafka.IAsyncDeserializer")));
                return count > 0;
            }
            catch (Exception e) when (
                e is ArgumentNullException ||
                e is TargetInvocationException ||
                e is MethodAccessException)
            {
                throw new AwsSchemaRegistryException("Can't find the class or instantiate it.", e);
            }
        }

        public object Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (obj is null)
            {
                throw new AwsSchemaRegistryException("Didn't find secondary deserializer.");
            }

            try
            {
                // We cannot invoke deserialize method with reflection because ReadOnlySpan cannot be converted to object
                var secDeserializer = (IDeserializer<object>)obj;
                return secDeserializer.Deserialize(data, isNull, context);
            }
            catch (Exception e) when (
                e is InvalidCastException ||
                e is ArgumentNullException ||
                e is TargetInvocationException ||
                e is MethodAccessException)
            {
                throw new AwsSchemaRegistryException("Can't find method called deserialize or invoke it.", e);
            }
        }

        public Task<object> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
        {
            if (obj is null)
            {
                throw new AwsSchemaRegistryException("Didn't find secondary deserializer.");
            }

            try
            {
                var secDeserialize = clz.GetMethod(
                    "DeserializeAsync",
                    new[] { typeof(ReadOnlyMemory<byte>), typeof(bool), typeof(SerializationContext) });
                var task = (Task<object>)secDeserialize!.Invoke(obj, new object[] { data, isNull, context });
                return task;
            }
            catch (Exception e) when (
                e is NullReferenceException||
                e is AmbiguousMatchException||
                e is ArgumentNullException ||
                e is ArgumentException ||
                e is TargetInvocationException ||
                e is MethodAccessException)
            {
                throw new AwsSchemaRegistryException("Can't find method called deserialize or invoke it.", e);
            }
        }
    }
}
