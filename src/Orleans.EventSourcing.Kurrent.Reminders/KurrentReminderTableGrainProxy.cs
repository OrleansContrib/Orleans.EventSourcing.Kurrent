
namespace Orleans.EventSourcing.Kurrent.Reminders;

internal sealed class KurrentReminderTableGrainProxy(IGrainFactory grainFactory) : IReminderTable
{
    private readonly IReminderTableGrain grain = grainFactory.GetGrain<IReminderTableGrain>(default(IdSpan));

    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
        => ReadRow(grainId, reminderName, CancellationToken.None);

    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName, CancellationToken cancellationToken)
        => grain.ReadRow(grainId, reminderName, cancellationToken);

    public Task<ReminderTableData> ReadRows(GrainId grainId)
        => ReadRows(grainId, CancellationToken.None);

    public Task<ReminderTableData> ReadRows(GrainId grainId, CancellationToken cancellationToken)
        => grain.ReadRows(grainId, cancellationToken);

    public Task<ReminderTableData> ReadRows(uint begin, uint end)
        => ReadRows(begin, end, CancellationToken.None);

    public Task<ReminderTableData> ReadRows(uint begin, uint end, CancellationToken cancellationToken)
        => grain.ReadRows(begin, end, cancellationToken);

    public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        => RemoveRow(grainId, reminderName, eTag, CancellationToken.None);

    public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag, CancellationToken cancellationToken)
        => grain.RemoveRow(grainId, reminderName, eTag, cancellationToken);

    public Task TestOnlyClearTable()
        => TestOnlyClearTable(CancellationToken.None);

    public Task TestOnlyClearTable(CancellationToken cancellationToken)
        => grain.TestOnlyClearTable(cancellationToken);

    public Task<string?> UpsertRow(ReminderEntry entry)
        => UpsertRow(entry, CancellationToken.None);

    public Task<string?> UpsertRow(ReminderEntry entry, CancellationToken cancellationToken)
        => grain.UpsertRow(entry, cancellationToken);
}
