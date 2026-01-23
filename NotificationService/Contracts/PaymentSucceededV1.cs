namespace NotificationService.Contracts;

public sealed record PaymentSucceededV1(
    long OrderId,
    decimal Price,
    DateTimeOffset DateCreate
);