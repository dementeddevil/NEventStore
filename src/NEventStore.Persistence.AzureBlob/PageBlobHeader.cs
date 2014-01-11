using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
    [Serializable]
    internal class PageBlobHeader
    {
        private List<PageBlobCommitDefinition> _pageBlobCommitDefinitions;

        public ReadOnlyCollection<PageBlobCommitDefinition> PageBlobCommitDefinitions
        {
            get
            { return new ReadOnlyCollection<PageBlobCommitDefinition>(_pageBlobCommitDefinitions); }
        }

        public PageBlobHeader()
        { _pageBlobCommitDefinitions = new List<PageBlobCommitDefinition>(); }

        public void AppendPageBlobCommitDefinition(PageBlobCommitDefinition definition)
        { _pageBlobCommitDefinitions.Add(definition); }
    }
}
