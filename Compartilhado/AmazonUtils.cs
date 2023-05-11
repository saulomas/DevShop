using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Compartilhado.Model;
using System.Collections.Generic;
using System.Text.Json;

namespace Compartilhado
{
    public static class AmazonUtils
    {
        const string queueUrl = "https://sqs.sa-east-1.amazonaws.com/480910489369";

        public static async Task SalvarAsync(this Pedido pedido)
        {
            var client = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
            var context = new DynamoDBContext(client);
            await context.SaveAsync(pedido);
        }

        public static T ToObject<T>(this Dictionary<string, AttributeValue> dictionary)
        {
            var client = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
            var context = new DynamoDBContext(client);

            var doc = Document.FromAttributeMap(dictionary);
            return context.FromDocument<T>(doc);
        }

        public static async Task EnviarParaFila(EnumFilasSQS fila, Pedido pedido)
        {
            var json = JsonSerializer.Serialize(pedido);
            var client = new AmazonSQSClient(RegionEndpoint.SAEast1);
            var request = new SendMessageRequest()
            {
                QueueUrl = $"{queueUrl}/{fila}",
                MessageBody = json
            };

            await client.SendMessageAsync(request);
        }

        public static async Task EnviarParaFila(EnumFilasSNS fila, Pedido pedido)
        {
            // Implementar
            await Task.CompletedTask;
        }
    }
}
