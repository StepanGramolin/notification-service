using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using NotificationService.WebApi.Contracts;
using NotificationService.WebApi.Hubs;

namespace NotificationService.WebApi.Infrastructure;

public sealed class PaymentEventsConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<PaymentEventsConsumer> _logger;
    private readonly IHubContext<NotificationsHub> _hub;

    public PaymentEventsConsumer(
        IConfiguration configuration,
        ILogger<PaymentEventsConsumer> logger,
        IHubContext<NotificationsHub> hub)
    {
        _configuration = configuration;
        _logger = logger;
        _hub = hub;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bootstrap = _configuration["Kafka:BootstrapServers"]
                        ?? throw new InvalidOperationException("Kafka:BootstrapServers is required");

        var topic = _configuration["Kafka:NotificationsTopic"] ?? "notifications";
        var groupId = _configuration["Kafka:GroupId"] ?? "notification-service";

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        _logger.LogInformation("Kafka consumer started. topic={Topic} groupId={GroupId}", topic, groupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? cr = null;

                try
                {
                    cr = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Consume failed");
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

                if (cr?.Message is null) continue;

                var correlationId = ReadHeader(cr.Message.Headers, "correlationId");
                var eventType = ReadHeader(cr.Message.Headers, "eventType");

                if (!string.Equals(eventType, "PaymentSucceededV1", StringComparison.Ordinal))
                {
                    _logger.LogWarning("Unknown eventType={EventType}. Skipping.", eventType);
                    consumer.Commit(cr);
                    continue;
                }

                PaymentSucceededV1? evt;
                try
                {
                    evt = JsonSerializer.Deserialize<PaymentSucceededV1>(cr.Message.Value);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to deserialize PaymentSucceededV1. value={Value}", cr.Message.Value);
                    consumer.Commit(cr);
                    continue;
                }

                if (evt is null)
                {
                    consumer.Commit(cr);
                    continue;
                }

                var notificationPayload = new
                {
                    type = eventType,
                    correlationId,
                    evt
                };

                await _hub.Clients.All.SendAsync("notification", JsonSerializer.Serialize(notificationPayload), stoppingToken);

                _logger.LogInformation(
                    "Forwarded event to SignalR. correlationId={CorrelationId} orderId={OrderId} partition={Partition} offset={Offset}",
                    correlationId, evt.OrderId, cr.Partition.Value, cr.Offset.Value);

                consumer.Commit(cr);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    private static string? ReadHeader(Headers? headers, string key)
    {
        if (headers is null) return null;
        var h = headers.FirstOrDefault(x => x.Key == key);
        return h is null ? null : Encoding.UTF8.GetString(h.GetValueBytes());
    }
}