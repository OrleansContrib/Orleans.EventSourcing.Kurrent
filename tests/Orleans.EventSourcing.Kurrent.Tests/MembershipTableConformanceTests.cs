using KurrentDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Clustering.TestKit;
using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Clustering;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

/// <summary>
///     Runs the Orleans clustering TestKit conformance suite against <see cref="KurrentMembershipTable"/>
///     with the default snapshot threshold.
/// </summary>
public sealed class MembershipTableConformanceTests() : MembershipTableConformanceTestsBase(KurrentClusteringOptions.DefaultEventCountBeforeSnapshot);

/// <summary>
///     Runs the conformance suite with a snapshot every couple of events, so every scenario also exercises
///     snapshot writes, stream truncation and reading a snapshot followed by later events.
/// </summary>
public sealed class MembershipTableFrequentSnapshotConformanceTests() : MembershipTableConformanceTestsBase(eventCountBeforeSnapshot: 2);

public abstract class MembershipTableConformanceTestsBase(int eventCountBeforeSnapshot)
{
    private const string ProviderName = "Kurrent";
    private const int Seed = 17;

    // Each fixture gets its own in-memory Kurrent backend shared by every table it creates,
    // so independently constructed tables coordinate through the same streams.
    private MembershipTableTestFixture CreateFixture()
    {
        var client = new InMemoryKurrentClient();
        var serviceId = $"kurrent-conformance-{Guid.NewGuid():N}";
        var clusteringOptions = new KurrentClusteringOptions { EventCountBeforeSnapshot = eventCountBeforeSnapshot };

        return new MembershipTableTestFixture(
            ProviderName,
            (fixtureServiceId, clusterId, _) =>
            {
                var services = new ServiceCollection()
                    .AddSingleton<IKurrentClient>(client)
                    .AddSingleton(Options.Create(clusteringOptions))
                    .AddSingleton(Options.Create(new ClusterOptions { ServiceId = fixtureServiceId, ClusterId = clusterId }))
                    .BuildServiceProvider();

                var table = new KurrentMembershipTable(KurrentMembershipEventStorageFactory.Create(services));
                return ValueTask.FromResult(new MembershipTableTestHandle(table, services.DisposeAsync));
            },
            (clusterId, cancellationToken) => IsDeletedAsync(client, $"{clusteringOptions.StreamPrefix}/{serviceId}/{clusterId}", cancellationToken),
            serviceId);
    }

    private static async ValueTask<bool> IsDeletedAsync(InMemoryKurrentClient client, string streamName, CancellationToken cancellationToken)
    {
        var readResult = await client.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start, 1, false, cancellationToken).ConfigureAwait(false);

        await foreach (var _ in readResult.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        return true;
    }

    private Task Run(Func<MembershipTableTestRunner, CancellationToken, Task> scenario)
        => CreateFixture().RunAsync((fixture, cancellationToken) => scenario(new MembershipTableTestRunner(fixture, Seed), cancellationToken),
                                    TestContext.Current.CancellationToken);

    [Fact]
    public Task G01_InsertRow_CurrentTableVersion_CommitsExactlyOneVersion()
        => Run((runner, ct) => runner.InsertRow_CurrentTableVersion_CommitsExactlyOneVersion(ct));

    [Fact]
    public Task G02_UpdateRow_CurrentTokens_CommitsExactlyOneVersion()
        => Run((runner, ct) => runner.UpdateRow_CurrentTokens_CommitsExactlyOneVersion(ct));

    [Fact]
    public Task G03_Reads_SameVersion_PreservesRetainedCanonicalFields()
        => Run((runner, ct) => runner.Reads_SameVersion_PreservesRetainedCanonicalFields(ct));

    [Fact]
    public Task G04_Reads_MaySkipCommittedVersions_WithoutSkippingHistoryValidation()
        => Run((runner, ct) => runner.Reads_MaySkipCommittedVersions_WithoutSkippingHistoryValidation(ct));

    [Fact]
    public Task G05_Lifecycle_DeadRemainsTerminalAfterCompaction_SuccessorUsesNewGeneration()
        => Run((runner, ct) => runner.Lifecycle_DeadRemainsTerminalAfterCompaction_SuccessorUsesNewGeneration(ct));

    [Fact]
    public Task G06_UpdateIAmAlive_OwnerWrites_PreserveCanonicalFieldsAndTableVersion()
        => Run((runner, ct) => runner.UpdateIAmAlive_OwnerWrites_PreserveCanonicalFieldsAndTableVersion(ct));

    [Fact]
    public Task G07_UpdateRow_TokensCapturedBeforeHeartbeat_CommitStatusChange()
        => Run((runner, ct) => runner.UpdateRow_TokensCapturedBeforeHeartbeat_CommitStatusChange(ct));

    [Fact]
    public Task G08_UpdateRow_TokensCapturedBeforeHeartbeat_CommitVoteChange()
        => Run((runner, ct) => runner.UpdateRow_TokensCapturedBeforeHeartbeat_CommitVoteChange(ct));

    [Fact]
    public Task G09_Handles_IndependentlyConstructed_ShareCommittedBackingState()
        => Run((runner, ct) => runner.Handles_IndependentlyConstructed_ShareCommittedBackingState(ct));

    [Fact]
    public Task G10_InsertRow_StaleTableToken_ReturnsFalseWithoutSideEffects()
        => Run((runner, ct) => runner.InsertRow_StaleTableToken_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task G11_UpdateRow_StaleTableTokenWithCurrentRowToken_ReturnsFalseWithoutSideEffects()
        => Run((runner, ct) => runner.UpdateRow_StaleTableTokenWithCurrentRowToken_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task G12_UpdateRow_StaleSnapshotAfterSameRowCommit_ReturnsFalseWithoutSideEffects()
        => Run((runner, ct) => runner.UpdateRow_StaleSnapshotAfterSameRowCommit_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task G13_InsertRow_DuplicateIdentity_ReturnsFalseWithoutSideEffects()
        => Run((runner, ct) => runner.InsertRow_DuplicateIdentity_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task G14_UpdateRow_MissingIdentityWithRealToken_ReturnsFalseWithoutSideEffects()
        => Run((runner, ct) => runner.UpdateRow_MissingIdentityWithRealToken_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task G15_ReadRow_AndReadAll_AgreeForPresentAndAbsentIdentities()
        => Run((runner, ct) => runner.ReadRow_AndReadAll_AgreeForPresentAndAbsentIdentities(ct));

    [Fact]
    public Task G16_Reads_RetainedObjectsRemainUnchangedAfterLaterWrites()
        => Run((runner, ct) => runner.Reads_RetainedObjectsRemainUnchangedAfterLaterWrites(ct));

    [Fact]
    public Task G17_InsertRow_MutatingInputAndSuspectList_DoesNotMutateStoredState()
        => Run((runner, ct) => runner.InsertRow_MutatingInputAndSuspectList_DoesNotMutateStoredState(ct));

    [Fact]
    public Task G18_UpdateRow_MutatingInputAndSuspectList_DoesNotMutateStoredState()
        => Run((runner, ct) => runner.UpdateRow_MutatingInputAndSuspectList_DoesNotMutateStoredState(ct));

    [Fact]
    public Task G19_Reads_MutatingReturnedEntryAndSuspectList_DoesNotMutateStoredState()
        => Run((runner, ct) => runner.Reads_MutatingReturnedEntryAndSuspectList_DoesNotMutateStoredState(ct));

    [Fact]
    public Task G20_ConcurrentCrossRowUpdates_SharedTableVersion_HaveExactlyOneWinner()
        => Run((runner, ct) => runner.ConcurrentCrossRowUpdates_SharedTableVersion_HaveExactlyOneWinner(ct));

    [Fact]
    public Task G21_ConcurrentReadAll_ReturnsOnlyAtomicCommittedViews()
        => Run((runner, ct) => runner.ConcurrentReadAll_ReturnsOnlyAtomicCommittedViews(ct));

    [Fact]
    public Task G22_ConcurrentReadRow_ReturnsOnlyAtomicCommittedViews()
        => Run((runner, ct) => runner.ConcurrentReadRow_ReturnsOnlyAtomicCommittedViews(ct));

    [Fact]
    public Task G23_InitializeMembershipTable_RepeatedWithData_PreservesCommittedState()
        => Run((runner, ct) => runner.InitializeMembershipTable_RepeatedWithData_PreservesCommittedState(ct));

    [Fact]
    public Task G24_Clusters_SharedBackendWithOverlappingSiloAddresses_AreIsolated()
        => Run((runner, ct) => runner.Clusters_SharedBackendWithOverlappingSiloAddresses_AreIsolated(ct));

    [Fact]
    public Task G25_CleanupDefunctSiloEntries_RemovesOnlyStrictlyOldDeadRows()
        => Run((runner, ct) => runner.CleanupDefunctSiloEntries_RemovesOnlyStrictlyOldDeadRows(ct));

    [Fact]
    public Task G26_DeleteMembershipTableEntries_DeletesOwnClusterAndPreservesOtherCluster()
        => Run((runner, ct) => runner.DeleteMembershipTableEntries_DeletesOwnClusterAndPreservesOtherCluster(ct));

    [Fact]
    public Task G27_DeleteMembershipTableEntries_DifferentClusterId_NeverDeletesConfiguredCluster()
        => Run((runner, ct) => runner.DeleteMembershipTableEntries_DifferentClusterId_NeverDeletesConfiguredCluster(ct));

    [Fact]
    public Task MembershipTable_ModelBased_GeneratedConformance()
        => new MembershipTableModelBasedTestRunner(
                CreateFixture,
                new MembershipTableModelBasedConformanceOptions
                {
                    ProviderName = ProviderName,
                    Seed = Seed,
                })
            .RunGeneratedConformanceTests(TestContext.Current.CancellationToken);
}
