using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;

using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests
{
    // Each provider's options are validated at startup, alongside the validators Orleans registers itself
    public sealed class ConfigurationValidationTests
    {
        private static readonly KurrentDBClientSettings ClientSettings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

        [Fact]
        public void ValidConfigurationPasses()
        {
            var services = ServicesWithOrleansValidator();
            services.AddKurrentBasedLogConsistencyProvider("events", o => o.Configure(Valid));
            services.AddKurrentBasedStorageProvider("state", o => o.Configure(Valid));
            services.AddKurrentBasedMembershipTable(o => o.Configure(x => x.ClientSettings = ClientSettings));

            Validate(services);
        }

        [Fact]
        public void LogConsistencyProviderIsValidated()
        {
            var services = ServicesWithOrleansValidator();
            services.AddKurrentBasedLogConsistencyProvider("events", o => o.Configure(x =>
            {
                Valid(x);
                x.Retry.BaseDelay = TimeSpan.Zero;
            }));

            var exception = Assert.Throws<OrleansConfigurationException>(() => Validate(services));
            Assert.Contains("events", exception.Message);
        }

        [Fact]
        public void GrainStorageProviderIsValidated()
        {
            var services = ServicesWithOrleansValidator();
            services.AddKurrentBasedStorageProvider("state", o => o.Configure(x =>
            {
                Valid(x);
                x.StreamNameProvider = null!;
            }));

            var exception = Assert.Throws<OrleansConfigurationException>(() => Validate(services));
            Assert.Contains("state", exception.Message);
        }

        [Fact]
        public void EveryNamedProviderIsValidated()
        {
            var services = ServicesWithOrleansValidator();
            services.AddKurrentBasedLogConsistencyProvider("first", o => o.Configure(Valid));
            services.AddKurrentBasedLogConsistencyProvider("second", o => o.Configure(x =>
            {
                Valid(x);
                x.ClientSettings = null!;
            }));

            var exception = Assert.Throws<OrleansConfigurationException>(() => Validate(services));
            Assert.Contains("second", exception.Message);
        }

        [Fact]
        public void MembershipTableIsValidated()
        {
            var services = ServicesWithOrleansValidator();
            services.AddKurrentBasedLogConsistencyProvider("events", o => o.Configure(Valid));
            services.AddKurrentBasedMembershipTable(o => o.Configure(x =>
            {
                x.ClientSettings = ClientSettings;
                x.StreamPrefix = " ";
            }));

            var exception = Assert.Throws<OrleansConfigurationException>(() => Validate(services));
            Assert.Contains(nameof(KurrentClusteringOptions.StreamPrefix), exception.Message);
        }

        [Theory]
        [InlineData(0, true)]
        [InlineData(-1, false)]
        public void SnapshotEveryWriteIsAllowedButNegativeCountsAreNot(int eventCountBeforeSnapshot, bool valid)
        {
            var services = ServicesWithOrleansValidator();
            services.AddKurrentBasedMembershipTable(o => o.Configure(x =>
            {
                x.ClientSettings = ClientSettings;
                x.EventCountBeforeSnapshot = eventCountBeforeSnapshot;
            }));

            if (valid)
            {
                Validate(services);
            }
            else
            {
                var exception = Assert.Throws<OrleansConfigurationException>(() => Validate(services));
                Assert.Contains("must not be negative", exception.Message);
            }
        }

        private static void Valid(KurrentStorageOptions options)
        {
            options.ClientSettings = ClientSettings;
            options.GrainStorageSerializer = new NoSerializer();
        }

        // Orleans registers its own validators before any provider's, so a provider's must not be skipped because one exists
        private static ServiceCollection ServicesWithOrleansValidator()
        {
            var services = new ServiceCollection();
            services.AddTransient<IConfigurationValidator, PassingValidator>();
            return services;
        }

        private static void Validate(ServiceCollection services)
        {
            using var provider = services.BuildServiceProvider();
            foreach (var validator in provider.GetServices<IConfigurationValidator>())
            {
                validator.ValidateConfiguration();
            }
        }

        private sealed class PassingValidator : IConfigurationValidator
        {
            public void ValidateConfiguration()
            {
            }
        }

        private sealed class NoSerializer : IGrainStorageSerializer
        {
            public BinaryData Serialize<T>(T input) => throw new NotSupportedException();

            public T Deserialize<T>(BinaryData input) => throw new NotSupportedException();
        }
    }
}
