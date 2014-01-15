using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// Holds the header infomation for a stream blob.
	/// </summary>
	[Serializable]
	internal class PageBlobHeader
	{
		private List<PageBlobCommitDefinition> _pageBlobCommitDefinitions;

		/// <summary>
		/// List of commits indices ( in _pageBlobCommitDefinitions ) that are undispatched. 
		/// </summary>
		public int UndispatchedCommitCount
		{ get; set; }

		/// <summary>
		/// A read only collection of page blob commit information.
		/// </summary>
		public ReadOnlyCollection<PageBlobCommitDefinition> PageBlobCommitDefinitions
		{
			get
			{ return new ReadOnlyCollection<PageBlobCommitDefinition>(_pageBlobCommitDefinitions); }
		}

		/// <summary>
		/// Creates a new PageBlobHeader.
		/// </summary>
		public PageBlobHeader()
		{ _pageBlobCommitDefinitions = new List<PageBlobCommitDefinition>(); }

		/// <summary>
		/// Adds a PageBlobCommitDefinition to the end of the list.
		/// </summary>
		/// <param name="definition">The definition to be added</param>
		public void AppendPageBlobCommitDefinition(PageBlobCommitDefinition definition)
		{ _pageBlobCommitDefinitions.Add(definition); }
	}
}
