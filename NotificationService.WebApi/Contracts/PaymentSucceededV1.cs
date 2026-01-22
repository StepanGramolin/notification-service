namespace NotificationService.WebApi.Contracts;

public sealed record PaymentSucceededV1(
    long PaymentId,
    long OrderId,
    decimal Price,
    DateTimeOffset OccurredAtUtc
);