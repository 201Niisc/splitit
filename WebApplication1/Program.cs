using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<AppData>();

var app = builder.Build();

app.UseStaticFiles();

// ── Groups ──────────────────────────────────────────────
app.MapGet("/api/groups", (AppData db) => db.Groups);

app.MapPost("/api/groups", (GroupInput input, AppData db) =>
{
    var g = new Group { Id = Guid.NewGuid(), Name = input.Name, Members = input.Members };
    db.Groups.Add(g);
    db.Save();
    return Results.Ok(g);
});

app.MapDelete("/api/groups/{id:guid}", (Guid id, AppData db) =>
{
    db.Groups.RemoveAll(g => g.Id == id);
    db.Expenses.RemoveAll(e => e.GroupId == id);
    db.Payments.RemoveAll(p => p.GroupId == id);
    db.ExpenseShares.RemoveAll(s => s.GroupId == id);
    db.Save();
    return Results.Ok();
});

// ── Expenses ─────────────────────────────────────────────
app.MapGet("/api/groups/{id:guid}/expenses", (Guid id, AppData db) =>
    db.Expenses.Where(e => e.GroupId == id));

app.MapPost("/api/groups/{id:guid}/expenses", (Guid id, ExpenseInput input, AppData db) =>
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
    db.Expenses.Add(e);
    db.Save();
    return Results.Ok(e);
});

app.MapDelete("/api/expenses/{id:guid}", (Guid id, AppData db) =>
{
    db.Expenses.RemoveAll(e => e.Id == id);
    db.ExpenseShares.RemoveAll(s => s.ExpenseId == id);
    db.Save();
    return Results.Ok();
});

// ── Expense Shares (per-person paid-back tracking) ────────
app.MapGet("/api/groups/{id:guid}/expense-shares", (Guid id, AppData db) =>
    db.ExpenseShares.Where(s => s.GroupId == id));

app.MapPost("/api/expenses/{id:guid}/shares", (Guid id, ShareInput input, AppData db) =>
{
    var expense = db.Expenses.FirstOrDefault(e => e.Id == id);
    if (expense is null) return Results.NotFound();
    if (db.ExpenseShares.Any(s => s.ExpenseId == id && s.Person == input.Person))
        return Results.Ok(); // already marked
    var s = new ExpenseShare { Id = Guid.NewGuid(), ExpenseId = id, GroupId = expense.GroupId, Person = input.Person, Date = DateTime.UtcNow };
    db.ExpenseShares.Add(s);
    db.Save();
    return Results.Ok(s);
});

app.MapDelete("/api/expense-shares/{id:guid}", (Guid id, AppData db) =>
{
    db.ExpenseShares.RemoveAll(s => s.Id == id);
    db.Save();
    return Results.Ok();
});

// ── Payments (settle up) ─────────────────────────────────
app.MapGet("/api/groups/{id:guid}/payments", (Guid id, AppData db) =>
    db.Payments.Where(p => p.GroupId == id));

app.MapPost("/api/groups/{id:guid}/payments", (Guid id, PaymentInput input, AppData db) =>
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
    db.Payments.Add(p);
    db.Save();
    return Results.Ok(p);
});

app.MapDelete("/api/payments/{id:guid}", (Guid id, AppData db) =>
{
    db.Payments.RemoveAll(p => p.Id == id);
    db.Save();
    return Results.Ok();
});

// ── Balances ─────────────────────────────────────────────
app.MapGet("/api/groups/{id:guid}/balances", (Guid id, AppData db) =>
{
    var group = db.Groups.FirstOrDefault(g => g.Id == id);
    if (group is null) return Results.NotFound();

    var expenses = db.Expenses.Where(e => e.GroupId == id).ToList();
    var payments = db.Payments.Where(p => p.GroupId == id).ToList();
    var expenseShares = db.ExpenseShares.Where(s => s.GroupId == id).ToList();
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

    // Simplify remaining debts
    var settlements = new List<Settlement>();
    var posArr = balances.Where(b => b.Value > 0.005).OrderByDescending(b => b.Value).Select(p => (p.Key, p.Value)).ToList();
    var negArr = balances.Where(b => b.Value < -0.005).OrderBy(b => b.Value).Select(p => (p.Key, p.Value)).ToList();

    int i = 0, j = 0;
    while (i < posArr.Count && j < negArr.Count)
    {
        var (creditor, credit) = posArr[i];
        var (debtor, debt) = negArr[j];
        double amount = Math.Min(credit, -debt);
        settlements.Add(new Settlement(debtor, creditor, Math.Round(amount, 2)));
        posArr[i] = (creditor, credit - amount);
        negArr[j] = (debtor, debt + amount);
        if (posArr[i].Item2 < 0.005) i++;
        if (-negArr[j].Item2 < 0.005) j++;
    }

    return Results.Ok(new
    {
        Balances = balances.ToDictionary(k => k.Key, k => Math.Round(k.Value, 2)),
        Settlements = settlements
    });
});

// ── Data version (for polling) ───────────────────────────
app.MapGet("/api/version", (AppData db) => Results.Ok(new { version = db.Version }));

// Serve index.html for all other routes
app.MapFallbackToFile("index.html");

app.Run();

// ── Models ───────────────────────────────────────────────
record GroupInput(string Name, List<string> Members);
record ExpenseInput(string Description, double Amount, string PaidBy, List<string> SplitAmong);
record PaymentInput(string From, string To, double Amount);
record ShareInput(string Person);
record Settlement(string From, string To, double Amount);

class Group
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> Members { get; set; } = [];
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

class AppData
{
    private readonly string _path = "data.json";
    public List<Group> Groups { get; set; } = [];
    public List<Expense> Expenses { get; set; } = [];
    public List<Payment> Payments { get; set; } = [];
    public List<ExpenseShare> ExpenseShares { get; set; } = [];
    public long Version { get; set; } = 0;

    public AppData()
    {
        if (File.Exists(_path))
        {
            var json = File.ReadAllText(_path);
            var data = JsonSerializer.Deserialize<AppDataDto>(json);
            if (data != null) { Groups = data.Groups; Expenses = data.Expenses; Payments = data.Payments; ExpenseShares = data.ExpenseShares; Version = data.Version; }
        }
    }

    public void Save()
    {
        Version++;
        File.WriteAllText(_path, JsonSerializer.Serialize(new AppDataDto { Groups = Groups, Expenses = Expenses, Payments = Payments, ExpenseShares = ExpenseShares, Version = Version }));
    }
}

class AppDataDto
{
    public List<Group> Groups { get; set; } = [];
    public List<Expense> Expenses { get; set; } = [];
    public List<Payment> Payments { get; set; } = [];
    public List<ExpenseShare> ExpenseShares { get; set; } = [];
    public long Version { get; set; } = 0;
}
