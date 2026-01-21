namespace NotificationService.WebApi.Contracts;

public sealed record PaymentSucceededV1(
    Guid PaymentId,
    Guid OrderId,
    decimal Amount,
    string Currency,
    DateTimeOffset OccurredAtUtc
);