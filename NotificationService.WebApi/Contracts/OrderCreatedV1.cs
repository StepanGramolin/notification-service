namespace NotificationService.WebApi.Contracts;

public sealed record OrderCreatedV1(
    long ProductId,
    int Amount,
    string EmailClient,
    decimal Price,
    string PhoneNumber
);