using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Compartilhado;
using Compartilhado.Model;
using System.Text.Json;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Reservador;

public class Function
{
    private AmazonDynamoDBClient AmazonDynamoDBClient { get; }
    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        AmazonDynamoDBClient = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
    /// to respond to SQS messages.
    /// </summary>
    /// <param name="evnt"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
    {
        if (evnt.Records.Count > 1) throw new InvalidOperationException("Somente uma mensagem pode ser tratada por vez.");
        var message = evnt.Records.FirstOrDefault();
        if (message == null) return;
        await ProcessMessageAsync(message, context);
    }

    private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
    {
        var pedido = JsonSerializer.Deserialize<Pedido>(message.Body);
        pedido.Status = StatusDoPedido.Reservado;

        foreach (var produto in pedido.Produtos)
        {
            try
            {
                await BaixarEstoque(produto.Id, produto.Quantidade);
                produto.Reservado = true;
                context.Logger.LogLine($"Produto baixado do estoque {produto.Id} - {produto.Nome}.");
            }
            catch (ConditionalCheckFailedException)
            {
                pedido.Cancelado = true;
                pedido.JustificativaDeCancelamento = $"Produto indisponível no estoque {produto.Id} - {produto.Nome}.";
                context.Logger.LogLine($"Erro: {pedido.JustificativaDeCancelamento}");
                break;
            }
        }

        if (pedido.Cancelado)
        {
            foreach (var produto in pedido.Produtos)
            {
                if (produto.Reservado)
                {
                    await DevolverAoEstoque(produto.Id, produto.Quantidade);
                    produto.Reservado = false;
                    context.Logger.LogLine($"Produto devolvido ao estoque {produto.Id} - {produto.Nome}");
                }
            }

            await AmazonUtils.EnviarParaFila(EnumFilasSNS.falha, pedido);            
        }
        else
        {
            await AmazonUtils.EnviarParaFila(EnumFilasSQS.reservado, pedido);
        }

        await pedido.SalvarAsync();
    }
    private async Task BaixarEstoque(string id, int quantidade)
    {
        var request = new UpdateItemRequest
        {
            TableName = "estoque",
            ReturnValues = "NONE",
            Key = new Dictionary<string, AttributeValue>
            {
                { "Id", new AttributeValue { S = id } }
            },
            UpdateExpression = "SET Quantidade = (Quantidade - :quantidadeDoPedido)",
            ConditionExpression = "Quantidade >= :quantidadeDoPedido",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":quantidadeDoPedido", new AttributeValue { N = quantidade.ToString() } }
            }
        };

        await AmazonDynamoDBClient.UpdateItemAsync(request);
    }

    private async Task DevolverAoEstoque(string id, int quantidade)
    {
        var request = new UpdateItemRequest
        {
            TableName = "estoque",
            ReturnValues = "NONE",
            Key = new Dictionary<string, AttributeValue>
            {
                { "Id", new AttributeValue { S = id } }
            },
            UpdateExpression = "SET Quantidade = (Quantidade + :quantidadeDoPedido)",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":quantidadeDoPedido", new AttributeValue { N = quantidade.ToString() } }
            }
        };

        await AmazonDynamoDBClient.UpdateItemAsync(request);
    }
}