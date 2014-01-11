using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
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
        /// Id of the commit
        /// </summary>
        public Guid CommitId
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
        /// 
        /// </summary>
        /// <param name="dataSizeBytes"></param>
        /// <param name="commitId"></param>
        /// <param name="revision"></param>
        public PageBlobCommitDefinition(int dataSizeBytes, Guid commitId, int revision)
        {
            DataSizeBytes = dataSizeBytes;
            CommitId = commitId;
            Revision = revision;
            IsDispatched = false;
        }
    }
}
