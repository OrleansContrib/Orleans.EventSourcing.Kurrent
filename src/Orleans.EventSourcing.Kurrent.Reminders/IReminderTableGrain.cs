using Orleans.CodeGeneration;

namespace Orleans.EventSourcing.Kurrent.Reminders;

[Alias("Orleans.EventSourcing.Reminders.IReminderTableGrain")]
[Version(1)]
internal interface IReminderTableGrain : IGrain, IReminderTable;