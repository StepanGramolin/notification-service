using NotificationService.Hubs;
using NotificationService.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSignalR();
builder.Services.AddHostedService<PaymentEventsConsumer>();
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.MapControllers();
app.MapGet("/health", () => Results.Ok("notification-service ok"));
app.MapHub<NotificationsHub>("/hubs/notifications");

app.Run();