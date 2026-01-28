using Npgmq;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();

const string defaultConnectionString = "Host=localhost;Username=postgres;Password=postgres;Database=npgmq_test;";
var connectionString = builder.Configuration.GetConnectionString("ExampleDB") ?? defaultConnectionString;

builder.Services.AddNpgmqClient(connectionString);

// OR...
// builder.Services.AddNpgsqlDataSource(connectionString);
// builder.Services.AddNpgmqClient();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseRouting();

app.UseAuthorization();

app.MapStaticAssets();
app.MapRazorPages()
    .WithStaticAssets();

// Perform some initialization on startup to make sure the queue we need exists...
using (var serviceScope = app.Services.CreateScope())
{
    var npgmqClient = serviceScope.ServiceProvider.GetRequiredService<INpgmqClient>();
    npgmqClient.InitAsync().Wait();
    npgmqClient.CreateQueueAsync("example_queue").Wait();
}

app.Run();