namespace Orleans.EventSourcing.Kurrent.Tests.Grains
{
    [GenerateSerializer]
    [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent")]
    public abstract record AccountEvent
    {
        [Id(0)]
        public Guid EventId { get; set; }
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Closed")]
        public sealed record Closed() : AccountEvent;
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Truncation")]
#pragma warning disable OEK0002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        [DiscardPriorEvents]
#pragma warning restore OEK0002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        public sealed record Truncation(decimal Balance) : AccountEvent;
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Deposited")]
        public sealed record Deposited(decimal Amount) : AccountEvent;
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Withdrawn")]
        public sealed record Withdrawn(decimal Amount) : AccountEvent;
    }

    [GenerateSerializer]
    [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountState")]
    public sealed record AccountState
    {
        public void Apply(AccountEvent.Deposited deposited)
        {
            Balance += deposited.Amount;
            Assert.True(Balance >= 0);
        }

        public void Apply(AccountEvent.Withdrawn withdrawn)
        {
            Balance -= withdrawn.Amount;
            Assert.True(Balance >= 0);
        }

        public void Apply(AccountEvent.Closed _)
        {

        }

        public void Apply(AccountEvent.Truncation truncation)
        {
            Balance = truncation.Balance;
        }

        [Id(0)]
        public decimal Balance { get; set; }
    }

    [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.IAccountGrain")]
    interface IAccountGrain : IGrainWithGuidKey
    {
        [Alias("Withdraw")]
        Task<bool> Withdraw(decimal amount, CancellationToken cancellationToken = default);
        [Alias("Deposit")]
        Task Deposit(decimal amount, CancellationToken cancellationToken = default);
        [Alias("DepositWithEventId")]
        Task Deposit(decimal amount, Guid eventId, CancellationToken cancellationToken = default);
        [Alias("DepositWithoutConfirm")]
        ValueTask DepositWithoutConfirm(decimal amount, CancellationToken cancellationToken = default);
        [Alias("TruncateAccountWithoutConfirm")]
        ValueTask TruncateAccountWithoutConfirm(CancellationToken cancellationToken = default);
        [Alias("ConfirmPendingEvents")]
        Task ConfirmPendingEvents(CancellationToken cancellationToken = default);
        [Alias("GetConfirmedBalance")]
        ValueTask<decimal> GetConfirmedBalance(CancellationToken cancellationToken = default);
        [Alias("GetTentativeBalance")]
        ValueTask<decimal> GetTentativeBalance(CancellationToken cancellationToken = default);
        [Alias("RefreshNow")]
        Task RefreshNow(CancellationToken cancellationToken = default);
        [Alias("CloseAccount")]
        Task<decimal> CloseAccount(CancellationToken cancellationToken = default);
        [Alias("GetEvents")]
        Task<IReadOnlyList<AccountEvent>> GetEvents(CancellationToken cancellationToken = default);
        [Alias("GetEventAtVersion")]
        Task<AccountEvent?> GetEventAtVersion(int version, CancellationToken cancellationToken = default);
        [Alias("GetConfirmedVersion")]
        Task<int> GetConfirmedVersion(CancellationToken cancellationToken = default);
        [Alias("ClearLog")]
        Task ClearLog(CancellationToken cancellationToken = default);
        [Alias("TruncateAccount")]
        Task TruncateAccount(CancellationToken cancellationToken = default);
    }

    internal class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain
    {
        public async Task<decimal> CloseAccount(CancellationToken cancellationToken)
        {
            var finalBalance = State.Balance;
            base.RaiseEvents<AccountEvent>([new AccountEvent.Withdrawn(finalBalance), new AccountEvent.Closed(),]);
            await ConfirmEvents();
            return finalBalance;
        }

        public async Task Deposit(decimal amount, CancellationToken cancellationToken)
        {
            RaiseEvent(new AccountEvent.Deposited(amount) { EventId = Guid.NewGuid(), });
            await ConfirmEvents();
        }

        public async Task Deposit(decimal amount, Guid eventId, CancellationToken cancellationToken)
        {
            RaiseEvent(new AccountEvent.Deposited(amount) { EventId = eventId, });
            await ConfirmEvents();
        }

        public ValueTask DepositWithoutConfirm(decimal amount, CancellationToken cancellationToken)
        {
            RaiseEvent(new AccountEvent.Deposited(amount) { EventId = Guid.NewGuid(), });
            return ValueTask.CompletedTask;
        }

        public ValueTask TruncateAccountWithoutConfirm(CancellationToken cancellationToken)
        {
            RaiseEvent(new AccountEvent.Truncation(State.Balance));
            return ValueTask.CompletedTask;
        }

        public Task ConfirmPendingEvents(CancellationToken cancellationToken) => ConfirmEvents();

        public ValueTask<decimal> GetConfirmedBalance(CancellationToken cancellationToken) => ValueTask.FromResult(State.Balance);

        public ValueTask<decimal> GetTentativeBalance(CancellationToken cancellationToken) => ValueTask.FromResult(TentativeState.Balance);

        public Task RefreshNow(CancellationToken cancellationToken) =>
            // call base class RefreshNow directly (bypass protected definition)
            base.RefreshNow();

        public Task<IReadOnlyList<AccountEvent>> GetEvents(CancellationToken cancellationToken) => base.RetrieveConfirmedEvents(0, this.Version);

        public async Task<AccountEvent?> GetEventAtVersion(int version, CancellationToken cancellationToken) =>
            (await base.RetrieveConfirmedEvents(version, version)).FirstOrDefault();

        public Task<int> GetConfirmedVersion(CancellationToken cancellationToken) => Task.FromResult(this.Version);

        public async Task<bool> Withdraw(decimal amount, CancellationToken cancellationToken)
        {
            if (State.Balance < amount)
            {
                return false;
            }

            return await base.RaiseConditionalEvent(new AccountEvent.Withdrawn(amount));
        }

        public Task ClearLog(CancellationToken cancellationToken) => base.ClearLogAsync(cancellationToken);

        public async Task TruncateAccount(CancellationToken cancellationToken)
        {
            RaiseEvent(new AccountEvent.Truncation(State.Balance));
            await ConfirmEvents();
        }
    }
}
