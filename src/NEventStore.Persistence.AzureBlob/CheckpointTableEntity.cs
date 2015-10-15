using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Entity used to house checkpoint incremental management
    /// </summary>
    public class CheckpointTableEntity : TableEntity
    {
        public string StreamId
        { get; set; }

        public int StreamRevision
        { get; set; }

        public Guid CommitId
        { get; set; }

        public int CommitSequence
        { get; set; }

        public ulong CheckpointTokenAsNumber
        { get; set; }

        public DateTime CommitDateTimeUtc
        { get; set; }

        public CheckpointTableEntity()
        { }

        public CheckpointTableEntity(ICommit commit)
        {
            var checkpointNumber = Convert.ToUInt64(commit.CheckpointToken);
            RowKey = commit.CheckpointToken;

            // figure out the range and build it out
            PartitionKey = ((int)(checkpointNumber / 1000)).ToString();
            StreamId = commit.StreamId;
            StreamRevision = commit.StreamRevision;
            CommitId = commit.CommitId;
            CommitSequence = commit.CommitSequence;
            CheckpointTokenAsNumber = checkpointNumber;
            CommitDateTimeUtc = commit.CommitStamp;
        }
    }
}
