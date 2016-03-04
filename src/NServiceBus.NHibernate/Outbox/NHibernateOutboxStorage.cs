namespace NServiceBus.Features
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Threading;
    using NHibernate.Mapping.ByCode;
    using NServiceBus.Outbox;
    using NServiceBus.Outbox.NHibernate;
    using Configuration = NHibernate.Cfg.Configuration;

    /// <summary>
    /// NHibernate Outbox Storage.
    /// </summary>
    public class NHibernateOutboxStorage : Feature
    {
        /// <summary>
        /// Creates an instance of <see cref="NHibernateOutboxStorage"/>.
        /// </summary>
        public NHibernateOutboxStorage()
        {
            DependsOn<Outbox>();
            RegisterStartupTask<OutboxCleaner>();
        }

        /// <summary>
        /// Called when the feature should perform its initialization. This call will only happen if the feature is enabled.
        /// </summary>
        protected override void Setup(FeatureConfigurationContext context)
        {
            context.Settings.Get<SharedMappings>()
                .AddMapping(ApplyMappings);

            context.Container.ConfigureComponent<OutboxPersister>(DependencyLifecycle.SingleInstance)
                .ConfigureProperty(op => op.EndpointName, context.Settings.EndpointName());
        }

        void ApplyMappings(Configuration config)
        {
            var mapper = new ModelMapper();
            mapper.AddMapping<OutboxEntityMap>();

            config.AddMapping(mapper.CompileMappingForAllExplicitlyAddedEntities());
        }

        class OutboxCleaner : FeatureStartupTask
        {
            public OutboxCleaner(OutboxPersister outboxPersister, CriticalError criticalError)
            {
                this.outboxPersister = outboxPersister;
                this.criticalError = criticalError;
            }

            protected override void OnStart()
            {
                var configValue = ConfigurationManager.AppSettings.Get("NServiceBus/Outbox/NHibernate/TimeToKeepDeduplicationData");

                if (configValue == null)
                {
                    timeToKeepDeduplicationData = TimeSpan.FromDays(7);
                }
                else
                {
                    if (!TimeSpan.TryParse(configValue, out timeToKeepDeduplicationData))
                    {
                        throw new Exception("Invalid value in \"NServiceBus/Outbox/NHibernate/TimeToKeepDeduplicationData\" AppSetting. Please ensure it is a TimeSpan.");
                    }
                }

                configValue = ConfigurationManager.AppSettings.Get("NServiceBus/Outbox/NHibernate/FrequencyToRunDeduplicationDataCleanup");

                if (configValue == null)
                {
                    frequencyToRunDeduplicationDataCleanup = TimeSpan.FromMinutes(1);
                }
                else
                {
                    if (!TimeSpan.TryParse(configValue, out frequencyToRunDeduplicationDataCleanup))
                    {
                        throw new Exception("Invalid value in \"NServiceBus/Outbox/NHibernate/FrequencyToRunDeduplicationDataCleanup\" AppSetting. Please ensure it is a TimeSpan.");
                    }
                }

                // Seed history
                var timestamp = DateTime.UtcNow;
                for (var i = 0; i < historyLength; i++)
                {
                    history.Add(Tuple.Create(batchSize, timestamp = timestamp - frequencyToRunDeduplicationDataCleanup));
                }

                cleanupIntervalMillisecondsMax = (int)frequencyToRunDeduplicationDataCleanup.TotalMilliseconds;
                cleanupTimer = new Timer(PerformCleanup, null, 0, Timeout.Infinite); // Trigger immediately at startup and just once
            }

            protected override void OnStop()
            {
                using (var waitHandle = new ManualResetEvent(false))
                {
                    cleanupTimer.Dispose(waitHandle);

                    waitHandle.WaitOne();
                }
            }

            void PerformCleanup(object state)
            {
                // Locking is not required to prevent
                // overlapping cleanups as the timer
                // will once fire once.

                try
                {
                    var timestamp = DateTime.UtcNow;
                    var count = outboxPersister.RemoveEntriesOlderThan(DateTime.UtcNow - timeToKeepDeduplicationData);

                    history.RemoveAt(0);
                    history.Add(Tuple.Create(count, timestamp));

                    var periodMilliseconds = (int)(history[historyLength - 1].Item2 - history[0].Item2).TotalMilliseconds; // 50.000
                    var totalCount = history.Sum(x => x.Item1) + 0; //25.000

                    // 2.000 = 1.000 [batchSize] * 50.000 [period] / 25.000 [totalCount]
                    var sleepDurationMilliseconds = periodMilliseconds / totalCount;

                    sleepDurationMilliseconds = Math.Min(sleepDurationMilliseconds, cleanupIntervalMillisecondsMax);
                    sleepDurationMilliseconds = Math.Max(sleepDurationMilliseconds, cleanupIntervalMillisecondsMin);

                    cleanupTimer.Change(sleepDurationMilliseconds, Timeout.Infinite); // Only trigger timeone once
                }
                catch (Exception ex)
                {
                    cleanupFailures++;
                    if (cleanupFailures >= 10)
                    {
                        criticalError.Raise("Failed to clean expired Outbox records after 10 consecutive unsuccessful attempts. The most likely cause of this is connectivity issues with your database.", ex);
                        cleanupFailures = 0;
                    }
                }
            }

            // ReSharper disable NotAccessedField.Local
            Timer cleanupTimer;
            // ReSharper restore NotAccessedField.Local
            OutboxPersister outboxPersister;
            CriticalError criticalError;
            int cleanupFailures;
            TimeSpan timeToKeepDeduplicationData;
            TimeSpan frequencyToRunDeduplicationDataCleanup;
            int cleanupIntervalMillisecondsMax;
            const int cleanupIntervalMillisecondsMin = 100;
            const int batchSize = 1000;
            const int historyLength = 25;
            readonly List<Tuple<int, DateTime>> history = new List<Tuple<int, DateTime>>(historyLength);
        }
    }
}
