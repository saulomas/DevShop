using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Compartilhado;
using Compartilhado.Model;
using Amazon;
using Amazon.DynamoDBv2.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Coletor;

public class Function
{
    public async Task FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
    {
        foreach (var record in dynamoEvent.Records)
        {
            if (record.EventName == "INSERT")
            {
                var pedido = record.Dynamodb.NewImage.ToObject<Pedido>();
                pedido.Status = StatusDoPedido.Coletado;

                try
                {
                    await ProcessarValorDoPedido(pedido, context);
                    await AmazonUtils.EnviarParaFila(EnumFilasSQS.pedido, pedido);
                    context.Logger.LogLine($"Sucesso na coleta do pedido: {pedido.Id}");
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Erro: {ex.Message}");
                    pedido.Cancelado = true;
                    pedido.JustificativaDeCancelamento = ex.Message;
                    await AmazonUtils.EnviarParaFila(EnumFilasSNS.falha, pedido);
                }

                await pedido.SalvarAsync();
            }
        }
    }

    private async Task ProcessarValorDoPedido(Pedido pedido, ILambdaContext context)
    {
        foreach (var produto in pedido.Produtos)
        {
            var produtoDoEstoque = await ObterProdutoDoDynamoDBAsync(produto.Id);
            if (produtoDoEstoque == null) throw new InvalidOperationException($"Produto n�o encontrado na tabela estoque. {produto.Id}");

            produto.Valor = produtoDoEstoque.Valor;
            produto.Nome = produtoDoEstoque.Nome;
        }

        var valorTotal = pedido.Produtos.Sum(x => x.Valor * x.Quantidade);
        if (pedido.ValorTotal != 0 && pedido.ValorTotal != valorTotal)
            throw new InvalidOperationException($"O valor esperado do pedido � de R$ {pedido.ValorTotal} e o valor verdadeiro � R$ {valorTotal}");

        pedido.ValorTotal = valorTotal;
    }

    private async Task<Produto?> ObterProdutoDoDynamoDBAsync(string id)
    {
        var client = new AmazonDynamoDBClient(RegionEndpoint.SAEast1);
        var request = new QueryRequest()
        {
            TableName = "estoque",
            KeyConditionExpression = "Id = :v_id",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":v_id", new AttributeValue { S = id } } }
        };

        var response = await client.QueryAsync(request);
        var item = response.Items.FirstOrDefault();
        if (item == null) return null;
        return item.ToObject<Produto>();
    }
}