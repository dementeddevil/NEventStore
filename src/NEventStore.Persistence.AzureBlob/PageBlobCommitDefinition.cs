using System;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Definition of a single commit contained within a page blob
    /// </summary>
    [Serializable]
    public class PageBlobCommitDefinition
    {
        // this is the size of an azure blob page
        private const int _pageSizeBytes = 512;

        /// <summary>
        /// Get the total number of bytes used for this commit
        /// </summary>
        public int DataSizeBytes
        { get; private set; }

        /// <summary>
        /// CheckPoint
        /// </summary>
        public ulong CheckPoint
        { get; private set; }

        /// <summary>
        /// Id of the commit
        /// </summary>
        public Guid CommitId
        { get; private set; }

        /// <summary>
        /// The utc time of the commit
        /// </summary>
        public DateTime CommitStampUtc
        { get; private set; }

        /// <summary>
        /// Get the revision
        /// </summary>
        public int Revision
        { get; private set; }

        /// <summary>
        /// Get if the commit has been dispatched
        /// </summary>
        public bool IsDispatched
        { get; set; }

        /// <summary>
        /// Get the total number of pages used by this commit
        /// </summary>
        public int TotalPagesUsed
        {
            get
            { return DataSizeBytes / 512 + 1; }
        }

        /// <summary>
        /// Create a new PageBlobCommitDefinition
        /// </summary>
        /// <param name="dataSizeBytes">Size of the blob in bytes.</param>
        /// <param name="commitId">Commit Id.</param>
        /// <param name="revision">Stream Revision.</param>
        /// <param name="commitStampUtc">Commit date/time stamp.</param>
        public PageBlobCommitDefinition(int dataSizeBytes, Guid commitId, int revision, DateTime commitStampUtc, ulong checkPoint)
        {
            DataSizeBytes = dataSizeBytes;
            CommitId = commitId;
            Revision = revision;
            CommitStampUtc = commitStampUtc;
            CheckPoint = checkPoint;
            IsDispatched = false;
        }
    }
}
