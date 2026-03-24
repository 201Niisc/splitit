using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// Resolve connection string: DATABASE_URL env var (Railway/Heroku format) takes priority,
// then ConnectionStrings:DefaultConnection from appsettings, then fall back to file storage.
var databaseUrl = Environment.GetEnvironmentVariable("DATABASE_URL");
var fallbackConnStr = builder.Configuration.GetConnectionString("DefaultConnection");

bool usePostgres = !string.IsNullOrWhiteSpace(databaseUrl) || !string.IsNullOrWhiteSpace(fallbackConnStr);
string? connStr = null;

if (!string.IsNullOrWhiteSpace(databaseUrl))
{
    // Parse Railway/Heroku postgresql://user:pass@host:port/dbname into Npgsql format
    var uri = new Uri(databaseUrl);
    var userInfo = uri.UserInfo.Split(':', 2);
    var user = userInfo[0];
    var password = userInfo.Length > 1 ? userInfo[1] : "";
    var host = uri.Host;
    var port = uri.Port > 0 ? uri.Port : 5432;
    var db = uri.AbsolutePath.TrimStart('/');
    connStr = $"Host={host};Port={port};Username={user};Password={password};Database={db};SSL Mode=Require;Trust Server Certificate=true";
}
else if (!string.IsNullOrWhiteSpace(fallbackConnStr))
{
    connStr = fallbackConnStr;
}

if (usePostgres && connStr != null)
{
    builder.Services.AddSingleton<IDataStore>(sp =>
    {
        var store = new Database(connStr);
        store.InitializeAsync().GetAwaiter().GetResult();
        return store;
    });
}
else
{
    builder.Services.AddSingleton<IDataStore, AppData>();
}

var app = builder.Build();

app.UseStaticFiles();

// ── Groups ──────────────────────────────────────────────
app.MapGet("/api/groups", async (IDataStore db) =>
{
    var groups = await db.GetGroupsAsync();
    return Results.Ok(groups.Select(g => new { g.Id, g.Name, g.Members, hasPin = g.PinHash != null }));
});

app.MapPost("/api/groups", async (GroupInput input, IDataStore db) =>
{
    var g = new Group
    {
        Id = Guid.NewGuid(),
        Name = input.Name,
        Members = input.Members,
        PinHash = string.IsNullOrWhiteSpace(input.Pin) ? null : HashPin(input.Pin)
    };
    await db.AddGroupAsync(g);
    return Results.Ok(new { g.Id, g.Name, g.Members, hasPin = g.PinHash != null });
});

app.MapPost("/api/groups/{id:guid}/verify-pin", async (Guid id, PinInput input, IDataStore db) =>
{
    var group = await db.GetGroupByIdAsync(id);
    if (group is null) return Results.NotFound();
    if (group.PinHash == null) return Results.Ok(new { ok = true });
    var match = group.PinHash == HashPin(input.Pin);
    return Results.Ok(new { ok = match });
});

app.MapPatch("/api/groups/{id:guid}/pin", async (Guid id, PinInput input, IDataStore db) =>
{
    var group = await db.GetGroupByIdAsync(id);
    if (group is null) return Results.NotFound();
    var newHash = string.IsNullOrWhiteSpace(input.Pin) ? null : HashPin(input.Pin);
    await db.SetGroupPinAsync(id, newHash);
    return Results.Ok();
});

app.MapDelete("/api/groups/{id:guid}", async (Guid id, IDataStore db) =>
{
    await db.DeleteGroupAsync(id);
    return Results.Ok();
});

// ── Expenses ─────────────────────────────────────────────
app.MapGet("/api/groups/{id:guid}/expenses", async (Guid id, IDataStore db) =>
    Results.Ok(await db.GetExpensesAsync(id)));

app.MapPost("/api/groups/{id:guid}/expenses", async (Guid id, ExpenseInput input, IDataStore db) =>
{
    var e = new Expense
    {
        Id = Guid.NewGuid(),
        GroupId = id,
        Description = input.Description,
        Amount = input.Amount,
        PaidBy = input.PaidBy,
        SplitAmong = input.SplitAmong,
        Date = DateTime.UtcNow
    };
    await db.AddExpenseAsync(e);
    return Results.Ok(e);
});

app.MapDelete("/api/expenses/{id:guid}", async (Guid id, IDataStore db) =>
{
    await db.DeleteExpenseAsync(id);
    return Results.Ok();
});

app.MapPatch("/api/expenses/{id:guid}", async (Guid id, ExpenseUpdateInput input, IDataStore db) =>
{
    await db.UpdateExpenseAsync(id, input);
    return Results.Ok();
});

// ── Expense Shares (per-person paid-back tracking) ────────
app.MapGet("/api/groups/{id:guid}/expense-shares", async (Guid id, IDataStore db) =>
    Results.Ok(await db.GetExpenseSharesAsync(id)));

app.MapPost("/api/expenses/{id:guid}/shares", async (Guid id, ShareInput input, IDataStore db) =>
{
    var expense = await db.GetExpenseByIdAsync(id);
    if (expense is null) return Results.NotFound();
    var existing = await db.GetExpenseSharesForExpenseAsync(id);
    if (existing.Any(s => s.Person == input.Person))
        return Results.Ok(); // already marked
    var s = new ExpenseShare
    {
        Id = Guid.NewGuid(),
        ExpenseId = id,
        GroupId = expense.GroupId,
        Person = input.Person,
        Date = DateTime.UtcNow
    };
    await db.AddExpenseShareAsync(s);
    return Results.Ok(s);
});

app.MapDelete("/api/expense-shares/{id:guid}", async (Guid id, IDataStore db) =>
{
    await db.DeleteExpenseShareAsync(id);
    return Results.Ok();
});

// ── Payments (settle up) ─────────────────────────────────
app.MapGet("/api/groups/{id:guid}/payments", async (Guid id, IDataStore db) =>
    Results.Ok(await db.GetPaymentsAsync(id)));

app.MapPost("/api/groups/{id:guid}/payments", async (Guid id, PaymentInput input, IDataStore db) =>
{
    var p = new Payment
    {
        Id = Guid.NewGuid(),
        GroupId = id,
        From = input.From,
        To = input.To,
        Amount = input.Amount,
        Date = DateTime.UtcNow
    };
    await db.AddPaymentAsync(p);
    return Results.Ok(p);
});

app.MapDelete("/api/payments/{id:guid}", async (Guid id, IDataStore db) =>
{
    await db.DeletePaymentAsync(id);
    return Results.Ok();
});

// ── Balances ─────────────────────────────────────────────
app.MapGet("/api/groups/{id:guid}/balances", async (Guid id, IDataStore db) =>
{
    var group = await db.GetGroupByIdAsync(id);
    if (group is null) return Results.NotFound();

    var expenses = await db.GetExpensesAsync(id);
    var payments = await db.GetPaymentsAsync(id);
    var expenseShares = await db.GetExpenseSharesAsync(id);

    var balances = new Dictionary<string, double>();
    foreach (var m in group.Members) balances[m] = 0;

    foreach (var exp in expenses)
    {
        double share = exp.Amount / exp.SplitAmong.Count;
        balances[exp.PaidBy] += exp.Amount;
        foreach (var m in exp.SplitAmong)
            balances[m] -= share;
    }

    // Apply marked-as-paid shares (person paid back their share directly)
    foreach (var s in expenseShares)
    {
        var exp = expenses.FirstOrDefault(e => e.Id == s.ExpenseId);
        if (exp is null) continue;
        double share = exp.Amount / exp.SplitAmong.Count;
        balances[s.Person] += share;
        balances[exp.PaidBy] -= share;
    }

    // Apply payments
    foreach (var pay in payments)
    {
        balances[pay.From] += pay.Amount;
        balances[pay.To] -= pay.Amount;
    }

    // Pair-wise debts: how much each person owes each other directly from expenses
    var pairDebt = new Dictionary<(string, string), double>();

    foreach (var exp in expenses)
    {
        double share = exp.Amount / exp.SplitAmong.Count;
        foreach (var m in exp.SplitAmong)
        {
            if (m == exp.PaidBy) continue;
            var key = (m, exp.PaidBy);
            pairDebt[key] = pairDebt.GetValueOrDefault(key, 0) + share;
        }
    }

    // Mark-as-paid reduces pair debt
    foreach (var s in expenseShares)
    {
        var exp = expenses.FirstOrDefault(e => e.Id == s.ExpenseId);
        if (exp is null) continue;
        double share = exp.Amount / exp.SplitAmong.Count;
        var key = (s.Person, exp.PaidBy);
        pairDebt[key] = pairDebt.GetValueOrDefault(key, 0) - share;
    }

    // Logged payments reduce pair debt
    foreach (var pay in payments)
    {
        var key = (pay.From, pay.To);
        pairDebt[key] = pairDebt.GetValueOrDefault(key, 0) - pay.Amount;
    }

    // Net each pair into a single settlement
    var settlements = new List<Settlement>();
    var processed = new HashSet<string>();

    foreach (var key in pairDebt.Keys.ToList())
    {
        var (a, b) = key;
        var pairKey = string.Join("|", new[] { a, b }.OrderBy(x => x));
        if (processed.Contains(pairKey)) continue;
        processed.Add(pairKey);

        double aOwesB = pairDebt.GetValueOrDefault((a, b), 0);
        double bOwesA = pairDebt.GetValueOrDefault((b, a), 0);
        double net = aOwesB - bOwesA;

        if (net > 0.005)
            settlements.Add(new Settlement(a, b, Math.Round(net, 2)));
        else if (net < -0.005)
            settlements.Add(new Settlement(b, a, Math.Round(-net, 2)));
    }

    return Results.Ok(new
    {
        Balances = balances.ToDictionary(k => k.Key, k => Math.Round(k.Value, 2)),
        Settlements = settlements
    });
});

// ── Data version (for polling) ───────────────────────────
app.MapGet("/api/version", async (IDataStore db) =>
    Results.Ok(new { version = await db.GetVersionAsync() }));

// Serve index.html for all other routes
app.MapFallbackToFile("index.html");

app.Run();

static string HashPin(string pin) =>
    Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(pin))).ToLower();

// ── Models ───────────────────────────────────────────────
record GroupInput(string Name, List<string> Members, string? Pin = null);
record PinInput(string Pin);
record ExpenseInput(string Description, double Amount, string PaidBy, List<string> SplitAmong);
record PaymentInput(string From, string To, double Amount);
record ShareInput(string Person);
record Settlement(string From, string To, double Amount);
record ExpenseUpdateInput(string Description, double Amount, string PaidBy, List<string> SplitAmong);

class Group
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> Members { get; set; } = [];
    public string? PinHash { get; set; }
}

class Expense
{
    public Guid Id { get; set; }
    public Guid GroupId { get; set; }
    public string Description { get; set; } = "";
    public double Amount { get; set; }
    public string PaidBy { get; set; } = "";
    public List<string> SplitAmong { get; set; } = [];
    public DateTime Date { get; set; }
}

class Payment
{
    public Guid Id { get; set; }
    public Guid GroupId { get; set; }
    public string From { get; set; } = "";
    public string To { get; set; } = "";
    public double Amount { get; set; }
    public DateTime Date { get; set; }
}

class ExpenseShare
{
    public Guid Id { get; set; }
    public Guid ExpenseId { get; set; }
    public Guid GroupId { get; set; }
    public string Person { get; set; } = "";
    public DateTime Date { get; set; }
}

// ── IDataStore interface ─────────────────────────────────
interface IDataStore
{
    Task<List<Group>> GetGroupsAsync();
    Task<Group?> GetGroupByIdAsync(Guid id);
    Task AddGroupAsync(Group group);
    Task DeleteGroupAsync(Guid id);

    Task<List<Expense>> GetExpensesAsync(Guid groupId);
    Task<Expense?> GetExpenseByIdAsync(Guid id);
    Task AddExpenseAsync(Expense expense);
    Task DeleteExpenseAsync(Guid id);
    Task UpdateExpenseAsync(Guid id, ExpenseUpdateInput update);

    Task<List<ExpenseShare>> GetExpenseSharesAsync(Guid groupId);
    Task<List<ExpenseShare>> GetExpenseSharesForExpenseAsync(Guid expenseId);
    Task AddExpenseShareAsync(ExpenseShare share);
    Task DeleteExpenseShareAsync(Guid id);

    Task<List<Payment>> GetPaymentsAsync(Guid groupId);
    Task AddPaymentAsync(Payment payment);
    Task DeletePaymentAsync(Guid id);

    Task SetGroupPinAsync(Guid id, string? pinHash);
    Task<long> GetVersionAsync();
}

// ── File-based AppData (local fallback) ──────────────────
class AppData : IDataStore
{
    private readonly string _path = "data.json";
    private List<Group> _groups = [];
    private List<Expense> _expenses = [];
    private List<Payment> _payments = [];
    private List<ExpenseShare> _expenseShares = [];
    private long _version = 0;

    public AppData()
    {
        if (File.Exists(_path))
        {
            var json = File.ReadAllText(_path);
            var data = JsonSerializer.Deserialize<AppDataDto>(json);
            if (data != null)
            {
                _groups = data.Groups;
                _expenses = data.Expenses;
                _payments = data.Payments;
                _expenseShares = data.ExpenseShares;
                _version = data.Version;
            }
        }
    }

    private void Save()
    {
        _version++;
        File.WriteAllText(_path, JsonSerializer.Serialize(new AppDataDto
        {
            Groups = _groups,
            Expenses = _expenses,
            Payments = _payments,
            ExpenseShares = _expenseShares,
            Version = _version
        }));
    }

    public Task<List<Group>> GetGroupsAsync() => Task.FromResult(_groups.ToList());
    public Task<Group?> GetGroupByIdAsync(Guid id) => Task.FromResult(_groups.FirstOrDefault(g => g.Id == id));

    public Task AddGroupAsync(Group group)
    {
        _groups.Add(group);
        Save();
        return Task.CompletedTask;
    }

    public Task DeleteGroupAsync(Guid id)
    {
        _groups.RemoveAll(g => g.Id == id);
        _expenses.RemoveAll(e => e.GroupId == id);
        _payments.RemoveAll(p => p.GroupId == id);
        _expenseShares.RemoveAll(s => s.GroupId == id);
        Save();
        return Task.CompletedTask;
    }

    public Task<List<Expense>> GetExpensesAsync(Guid groupId) =>
        Task.FromResult(_expenses.Where(e => e.GroupId == groupId).ToList());

    public Task<Expense?> GetExpenseByIdAsync(Guid id) =>
        Task.FromResult(_expenses.FirstOrDefault(e => e.Id == id));

    public Task AddExpenseAsync(Expense expense)
    {
        _expenses.Add(expense);
        Save();
        return Task.CompletedTask;
    }

    public Task DeleteExpenseAsync(Guid id)
    {
        _expenses.RemoveAll(e => e.Id == id);
        _expenseShares.RemoveAll(s => s.ExpenseId == id);
        Save();
        return Task.CompletedTask;
    }

    public Task UpdateExpenseAsync(Guid id, ExpenseUpdateInput update)
    {
        var e = _expenses.FirstOrDefault(x => x.Id == id);
        if (e != null) { e.Description = update.Description; e.Amount = update.Amount; e.PaidBy = update.PaidBy; e.SplitAmong = update.SplitAmong; Save(); }
        return Task.CompletedTask;
    }

    public Task<List<ExpenseShare>> GetExpenseSharesAsync(Guid groupId) =>
        Task.FromResult(_expenseShares.Where(s => s.GroupId == groupId).ToList());

    public Task<List<ExpenseShare>> GetExpenseSharesForExpenseAsync(Guid expenseId) =>
        Task.FromResult(_expenseShares.Where(s => s.ExpenseId == expenseId).ToList());

    public Task AddExpenseShareAsync(ExpenseShare share)
    {
        _expenseShares.Add(share);
        Save();
        return Task.CompletedTask;
    }

    public Task DeleteExpenseShareAsync(Guid id)
    {
        _expenseShares.RemoveAll(s => s.Id == id);
        Save();
        return Task.CompletedTask;
    }

    public Task<List<Payment>> GetPaymentsAsync(Guid groupId) =>
        Task.FromResult(_payments.Where(p => p.GroupId == groupId).ToList());

    public Task AddPaymentAsync(Payment payment)
    {
        _payments.Add(payment);
        Save();
        return Task.CompletedTask;
    }

    public Task DeletePaymentAsync(Guid id)
    {
        _payments.RemoveAll(p => p.Id == id);
        Save();
        return Task.CompletedTask;
    }

    public Task SetGroupPinAsync(Guid id, string? pinHash)
    {
        var g = _groups.FirstOrDefault(x => x.Id == id);
        if (g != null) { g.PinHash = pinHash; Save(); }
        return Task.CompletedTask;
    }

    public Task<long> GetVersionAsync() => Task.FromResult(_version);
}

class AppDataDto
{
    public List<Group> Groups { get; set; } = [];
    public List<Expense> Expenses { get; set; } = [];
    public List<Payment> Payments { get; set; } = [];
    public List<ExpenseShare> ExpenseShares { get; set; } = [];
    public long Version { get; set; } = 0;
}

// ── PostgreSQL Database class ────────────────────────────
class Database : IDataStore
{
    private readonly string _connStr;

    // Seed data (data.json contents baked in for first-run migration)
    private const string SeedJson = """
        {"Groups":[{"Id":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Name":"Spain trip","Members":["Nisanth","Brendon","Souvik","Thulasi","Rajkumar"]}],"Expenses":[{"Id":"f715f370-985f-4734-a268-4a31f66f0d0c","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Description":"Granada Stay","Amount":122.0,"PaidBy":"Thulasi","SplitAmong":["Nisanth","Brendon","Souvik","Thulasi","Rajkumar"],"Date":"2026-03-17T01:45:00.3368725Z"},{"Id":"2d569d8f-c577-4f6d-958b-21ea5275d483","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Description":"Ronda Stay","Amount":109.35,"PaidBy":"Nisanth","SplitAmong":["Nisanth","Brendon","Souvik","Thulasi","Rajkumar"],"Date":"2026-03-17T01:46:48.8598448Z"},{"Id":"2ce99629-a915-4e25-90c3-a97e03a4028d","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Description":"Bilbao Stay","Amount":281.9,"PaidBy":"Nisanth","SplitAmong":["Nisanth","Brendon","Souvik","Thulasi","Rajkumar"],"Date":"2026-03-17T01:47:06.170721Z"},{"Id":"9b28135d-2905-431c-9fdf-b7489c04f9f4","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Description":"Car","Amount":458.0,"PaidBy":"Nisanth","SplitAmong":["Nisanth","Brendon","Souvik","Thulasi","Rajkumar"],"Date":"2026-03-17T01:47:24.4882165Z"},{"Id":"1e70e44f-89a5-444d-a853-e9f2730c9811","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Description":"Car Insurance","Amount":300.0,"PaidBy":"Nisanth","SplitAmong":["Nisanth","Brendon","Souvik","Thulasi","Rajkumar"],"Date":"2026-03-17T18:37:14.4770149Z"}],"Payments":[],"ExpenseShares":[{"Id":"a162d22f-b32f-4fc6-8912-a7be6b6b156b","ExpenseId":"1e70e44f-89a5-444d-a853-e9f2730c9811","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Person":"Brendon","Date":"2026-03-17T18:37:44.8499199Z"},{"Id":"58f3de3b-79d2-48cc-b168-dd25627c0adb","ExpenseId":"9b28135d-2905-431c-9fdf-b7489c04f9f4","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Person":"Brendon","Date":"2026-03-17T18:37:46.7413821Z"},{"Id":"8270725e-a4e7-4b19-9ae7-822a697bf771","ExpenseId":"2ce99629-a915-4e25-90c3-a97e03a4028d","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Person":"Brendon","Date":"2026-03-17T18:37:48.657344Z"},{"Id":"b59e2723-e15a-4461-9215-5161a5d82062","ExpenseId":"2d569d8f-c577-4f6d-958b-21ea5275d483","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Person":"Brendon","Date":"2026-03-17T18:37:50.5358521Z"},{"Id":"2c39667d-7263-43da-972f-f1d1df4771e1","ExpenseId":"f715f370-985f-4734-a268-4a31f66f0d0c","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Person":"Brendon","Date":"2026-03-17T18:37:57.0238128Z"},{"Id":"841f295b-5b2a-4a6f-8a28-bc562f5c6295","ExpenseId":"f715f370-985f-4734-a268-4a31f66f0d0c","GroupId":"a0cb8d3e-316e-4a53-94d8-862d51a3f9a5","Person":"Nisanth","Date":"2026-03-17T18:40:37.2761123Z"}],"Version":26}
        """;

    public Database(string connStr)
    {
        _connStr = connStr;
    }

    private NpgsqlConnection CreateConnection() => new NpgsqlConnection(_connStr + ";Timeout=10;Command Timeout=10");

    public async Task InitializeAsync()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();

        // Create tables
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IF NOT EXISTS groups (
                    id UUID PRIMARY KEY,
                    name TEXT NOT NULL,
                    members TEXT NOT NULL,
                    pin_hash TEXT NULL
                );
                ALTER TABLE groups ADD COLUMN IF NOT EXISTS pin_hash TEXT NULL;

                CREATE TABLE IF NOT EXISTS expenses (
                    id UUID PRIMARY KEY,
                    group_id UUID NOT NULL,
                    description TEXT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    paid_by TEXT NOT NULL,
                    split_among TEXT NOT NULL,
                    date TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id UUID PRIMARY KEY,
                    group_id UUID NOT NULL,
                    "from" TEXT NOT NULL,
                    "to" TEXT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    date TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS expense_shares (
                    id UUID PRIMARY KEY,
                    expense_id UUID NOT NULL,
                    group_id UUID NOT NULL,
                    person TEXT NOT NULL,
                    date TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_version (
                    id INT PRIMARY KEY DEFAULT 1,
                    version BIGINT NOT NULL DEFAULT 0,
                    CONSTRAINT single_row CHECK (id = 1)
                );

                INSERT INTO app_version (id, version) VALUES (1, 0) ON CONFLICT DO NOTHING;
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        // Seed from data.json if groups table is empty
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM groups";
            var count = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
            if (count == 0)
            {
                await SeedDataAsync(conn);
            }
        }
    }

    private async Task SeedDataAsync(NpgsqlConnection conn)
    {
        // Try data.json file first, fall back to embedded seed
        string json;
        if (File.Exists("data.json"))
            json = await File.ReadAllTextAsync("data.json");
        else
            json = SeedJson;

        var data = JsonSerializer.Deserialize<AppDataDto>(json);
        if (data == null) return;

        await using var tx = await conn.BeginTransactionAsync();

        foreach (var g in data.Groups)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO groups (id, name, members) VALUES (@id, @name, @members) ON CONFLICT DO NOTHING";
            cmd.Parameters.AddWithValue("id", g.Id);
            cmd.Parameters.AddWithValue("name", g.Name);
            cmd.Parameters.AddWithValue("members", JsonSerializer.Serialize(g.Members));
            await cmd.ExecuteNonQueryAsync();
        }

        foreach (var e in data.Expenses)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO expenses (id, group_id, description, amount, paid_by, split_among, date) VALUES (@id, @gid, @desc, @amt, @paidby, @split, @date) ON CONFLICT DO NOTHING";
            cmd.Parameters.AddWithValue("id", e.Id);
            cmd.Parameters.AddWithValue("gid", e.GroupId);
            cmd.Parameters.AddWithValue("desc", e.Description);
            cmd.Parameters.AddWithValue("amt", e.Amount);
            cmd.Parameters.AddWithValue("paidby", e.PaidBy);
            cmd.Parameters.AddWithValue("split", JsonSerializer.Serialize(e.SplitAmong));
            cmd.Parameters.AddWithValue("date", e.Date);
            await cmd.ExecuteNonQueryAsync();
        }

        foreach (var p in data.Payments)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO payments (id, group_id, \"from\", \"to\", amount, date) VALUES (@id, @gid, @from, @to, @amt, @date) ON CONFLICT DO NOTHING";
            cmd.Parameters.AddWithValue("id", p.Id);
            cmd.Parameters.AddWithValue("gid", p.GroupId);
            cmd.Parameters.AddWithValue("from", p.From);
            cmd.Parameters.AddWithValue("to", p.To);
            cmd.Parameters.AddWithValue("amt", p.Amount);
            cmd.Parameters.AddWithValue("date", p.Date);
            await cmd.ExecuteNonQueryAsync();
        }

        foreach (var s in data.ExpenseShares)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO expense_shares (id, expense_id, group_id, person, date) VALUES (@id, @eid, @gid, @person, @date) ON CONFLICT DO NOTHING";
            cmd.Parameters.AddWithValue("id", s.Id);
            cmd.Parameters.AddWithValue("eid", s.ExpenseId);
            cmd.Parameters.AddWithValue("gid", s.GroupId);
            cmd.Parameters.AddWithValue("person", s.Person);
            cmd.Parameters.AddWithValue("date", s.Date);
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "UPDATE app_version SET version = @v WHERE id = 1";
            cmd.Parameters.AddWithValue("v", data.Version);
            await cmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }

    private async Task BumpVersionAsync(NpgsqlConnection conn)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE app_version SET version = version + 1 WHERE id = 1";
        await cmd.ExecuteNonQueryAsync();
    }

    // ── Groups ──────────────────────────────────────────────
    public async Task<List<Group>> GetGroupsAsync()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, members, pin_hash FROM groups ORDER BY name";
        await using var reader = await cmd.ExecuteReaderAsync();
        var result = new List<Group>();
        while (await reader.ReadAsync())
        {
            result.Add(new Group
            {
                Id = reader.GetGuid(0),
                Name = reader.GetString(1),
                Members = JsonSerializer.Deserialize<List<string>>(reader.GetString(2)) ?? [],
                PinHash = reader.IsDBNull(3) ? null : reader.GetString(3)
            });
        }
        return result;
    }

    public async Task<Group?> GetGroupByIdAsync(Guid id)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, members, pin_hash FROM groups WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;
        return new Group
        {
            Id = reader.GetGuid(0),
            Name = reader.GetString(1),
            Members = JsonSerializer.Deserialize<List<string>>(reader.GetString(2)) ?? [],
            PinHash = reader.IsDBNull(3) ? null : reader.GetString(3)
        };
    }

    public async Task AddGroupAsync(Group group)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO groups (id, name, members, pin_hash) VALUES (@id, @name, @members, @pin)";
        cmd.Parameters.AddWithValue("id", group.Id);
        cmd.Parameters.AddWithValue("name", group.Name);
        cmd.Parameters.AddWithValue("members", JsonSerializer.Serialize(group.Members));
        cmd.Parameters.AddWithValue("pin", (object?)group.PinHash ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    public async Task DeleteGroupAsync(Guid id)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();
        foreach (var sql in new[]
        {
            "DELETE FROM expense_shares WHERE group_id = @id",
            "DELETE FROM payments WHERE group_id = @id",
            "DELETE FROM expenses WHERE group_id = @id",
            "DELETE FROM groups WHERE id = @id"
        })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync();
        }
        await BumpVersionAsync(conn);
        await tx.CommitAsync();
    }

    // ── Expenses ─────────────────────────────────────────────
    public async Task<List<Expense>> GetExpensesAsync(Guid groupId)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, group_id, description, amount, paid_by, split_among, date FROM expenses WHERE group_id = @gid ORDER BY date";
        cmd.Parameters.AddWithValue("gid", groupId);
        await using var reader = await cmd.ExecuteReaderAsync();
        var result = new List<Expense>();
        while (await reader.ReadAsync())
        {
            result.Add(new Expense
            {
                Id = reader.GetGuid(0),
                GroupId = reader.GetGuid(1),
                Description = reader.GetString(2),
                Amount = reader.GetDouble(3),
                PaidBy = reader.GetString(4),
                SplitAmong = JsonSerializer.Deserialize<List<string>>(reader.GetString(5)) ?? [],
                Date = reader.GetDateTime(6)
            });
        }
        return result;
    }

    public async Task<Expense?> GetExpenseByIdAsync(Guid id)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, group_id, description, amount, paid_by, split_among, date FROM expenses WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;
        return new Expense
        {
            Id = reader.GetGuid(0),
            GroupId = reader.GetGuid(1),
            Description = reader.GetString(2),
            Amount = reader.GetDouble(3),
            PaidBy = reader.GetString(4),
            SplitAmong = JsonSerializer.Deserialize<List<string>>(reader.GetString(5)) ?? [],
            Date = reader.GetDateTime(6)
        };
    }

    public async Task AddExpenseAsync(Expense expense)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO expenses (id, group_id, description, amount, paid_by, split_among, date) VALUES (@id, @gid, @desc, @amt, @paidby, @split, @date)";
        cmd.Parameters.AddWithValue("id", expense.Id);
        cmd.Parameters.AddWithValue("gid", expense.GroupId);
        cmd.Parameters.AddWithValue("desc", expense.Description);
        cmd.Parameters.AddWithValue("amt", expense.Amount);
        cmd.Parameters.AddWithValue("paidby", expense.PaidBy);
        cmd.Parameters.AddWithValue("split", JsonSerializer.Serialize(expense.SplitAmong));
        cmd.Parameters.AddWithValue("date", expense.Date);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    public async Task DeleteExpenseAsync(Guid id)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM expense_shares WHERE expense_id = @id";
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM expenses WHERE id = @id";
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync();
        }
        await BumpVersionAsync(conn);
        await tx.CommitAsync();
    }

    public async Task UpdateExpenseAsync(Guid id, ExpenseUpdateInput update)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE expenses SET description = @desc, amount = @amt, paid_by = @paidby, split_among = @split WHERE id = @id";
        cmd.Parameters.AddWithValue("desc", update.Description);
        cmd.Parameters.AddWithValue("amt", update.Amount);
        cmd.Parameters.AddWithValue("paidby", update.PaidBy);
        cmd.Parameters.AddWithValue("split", JsonSerializer.Serialize(update.SplitAmong));
        cmd.Parameters.AddWithValue("id", id);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    // ── Expense Shares ────────────────────────────────────────
    public async Task<List<ExpenseShare>> GetExpenseSharesAsync(Guid groupId)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, expense_id, group_id, person, date FROM expense_shares WHERE group_id = @gid ORDER BY date";
        cmd.Parameters.AddWithValue("gid", groupId);
        return await ReadExpenseSharesAsync(cmd);
    }

    public async Task<List<ExpenseShare>> GetExpenseSharesForExpenseAsync(Guid expenseId)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, expense_id, group_id, person, date FROM expense_shares WHERE expense_id = @eid";
        cmd.Parameters.AddWithValue("eid", expenseId);
        return await ReadExpenseSharesAsync(cmd);
    }

    private static async Task<List<ExpenseShare>> ReadExpenseSharesAsync(NpgsqlCommand cmd)
    {
        await using var reader = await cmd.ExecuteReaderAsync();
        var result = new List<ExpenseShare>();
        while (await reader.ReadAsync())
        {
            result.Add(new ExpenseShare
            {
                Id = reader.GetGuid(0),
                ExpenseId = reader.GetGuid(1),
                GroupId = reader.GetGuid(2),
                Person = reader.GetString(3),
                Date = reader.GetDateTime(4)
            });
        }
        return result;
    }

    public async Task AddExpenseShareAsync(ExpenseShare share)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO expense_shares (id, expense_id, group_id, person, date) VALUES (@id, @eid, @gid, @person, @date)";
        cmd.Parameters.AddWithValue("id", share.Id);
        cmd.Parameters.AddWithValue("eid", share.ExpenseId);
        cmd.Parameters.AddWithValue("gid", share.GroupId);
        cmd.Parameters.AddWithValue("person", share.Person);
        cmd.Parameters.AddWithValue("date", share.Date);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    public async Task DeleteExpenseShareAsync(Guid id)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM expense_shares WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    // ── Payments ─────────────────────────────────────────────
    public async Task<List<Payment>> GetPaymentsAsync(Guid groupId)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, group_id, \"from\", \"to\", amount, date FROM payments WHERE group_id = @gid ORDER BY date";
        cmd.Parameters.AddWithValue("gid", groupId);
        await using var reader = await cmd.ExecuteReaderAsync();
        var result = new List<Payment>();
        while (await reader.ReadAsync())
        {
            result.Add(new Payment
            {
                Id = reader.GetGuid(0),
                GroupId = reader.GetGuid(1),
                From = reader.GetString(2),
                To = reader.GetString(3),
                Amount = reader.GetDouble(4),
                Date = reader.GetDateTime(5)
            });
        }
        return result;
    }

    public async Task AddPaymentAsync(Payment payment)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO payments (id, group_id, \"from\", \"to\", amount, date) VALUES (@id, @gid, @from, @to, @amt, @date)";
        cmd.Parameters.AddWithValue("id", payment.Id);
        cmd.Parameters.AddWithValue("gid", payment.GroupId);
        cmd.Parameters.AddWithValue("from", payment.From);
        cmd.Parameters.AddWithValue("to", payment.To);
        cmd.Parameters.AddWithValue("amt", payment.Amount);
        cmd.Parameters.AddWithValue("date", payment.Date);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    public async Task DeletePaymentAsync(Guid id)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM payments WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    // ── Pin ───────────────────────────────────────────────────
    public async Task SetGroupPinAsync(Guid id, string? pinHash)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE groups SET pin_hash = @pin WHERE id = @id";
        cmd.Parameters.AddWithValue("pin", (object?)pinHash ?? DBNull.Value);
        cmd.Parameters.AddWithValue("id", id);
        await cmd.ExecuteNonQueryAsync();
        await BumpVersionAsync(conn);
    }

    // ── Version ───────────────────────────────────────────────
    public async Task<long> GetVersionAsync()
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT version FROM app_version WHERE id = 1";
        return (long)(await cmd.ExecuteScalarAsync() ?? 0L);
    }
}
