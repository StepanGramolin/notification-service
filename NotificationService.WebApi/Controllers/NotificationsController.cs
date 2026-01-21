using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using NotificationService.WebApi.Contracts;
using NotificationService.WebApi.Hubs;

namespace NotificationService.WebApi.Controllers;

[ApiController]
[Route("api/notifications")]
public sealed class NotificationsController : ControllerBase
{
    private readonly IHubContext<NotificationsHub> _hub;

    public NotificationsController(IHubContext<NotificationsHub> hub)
    {
        _hub = hub;
    }

    // POST /api/notifications/broadcast
    [HttpPost("broadcast")]
    public async Task<IActionResult> Broadcast([FromBody] BroadcastNotificationRequest request, CancellationToken ct)
    {
        await _hub.Clients.All.SendAsync("notification", request.Message, ct);
        return Ok(new { sent = true, request.Message });
    }
}