using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Npgmq.Example.Web.Pages;

public class IndexModel(INpgmqClient npgmqClient) : PageModel
{
    public const string QueueName = "example_queue";

    public class MyMessageType
    {
        public string Foo { get; init; } = null!;
        public int Bar { get; init; }
    }

    [BindProperty] public MyMessageType MessageToSend { get; set; } = new();
    [BindProperty] public long? SentMessageId { get; set; }
    [BindProperty] public NpgmqMessage<MyMessageType>? PopResult { get; set; }

    public void OnGet()
    {
        MessageToSend = new MyMessageType();
    }

    public async Task<IActionResult> OnPostSendAsync()
    {
        PopResult = null;
        if (!ModelState.IsValid)
        {
            return Page();
        }
        ModelState.Clear();
        SentMessageId = await npgmqClient.SendAsync(QueueName, MessageToSend);
        MessageToSend = new MyMessageType();
        return Page();
    }

    public async Task<IActionResult> OnPostPopAsync()
    {
        PopResult = await npgmqClient.PopAsync<MyMessageType>(QueueName);
        return Page();
    }
}