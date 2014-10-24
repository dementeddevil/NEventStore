using System;
using System.IO;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// Definition of the header
	/// </summary>
	internal class HeaderDefinitionMetadata
	{
		public const int RawSize = 9;

		/// <summary>
		/// Get the start location of the header in bytes
		/// </summary>
		public int HeaderStartLocationOffsetBytes
		{ get; set; }

		/// <summary>
		/// Get the size of the header in bytes
		/// </summary>
		public int HeaderSizeInBytes
		{ get; set; }

		/// <summary>
		/// Get if there are undispatched commits
		/// </summary>
		public bool HasUndispatchedCommits
		{ get; set; }

		/// <summary>
		/// Get the raw data.
		/// </summary>
		/// <returns></returns>
		public byte[] GetRaw()
		{
			using (var ms = new MemoryStream())
			{
				WriteToMs(ms, BitConverter.GetBytes(HeaderStartLocationOffsetBytes));
				WriteToMs(ms, BitConverter.GetBytes(HeaderSizeInBytes));
				WriteToMs(ms, BitConverter.GetBytes(HasUndispatchedCommits));
				return ms.ToArray();
			}
		}

		/// <summary>
		/// Create a header definition from raw data
		/// </summary>
		/// <param name="raw"></param>
		/// <returns></returns>
		public static HeaderDefinitionMetadata FromRaw(byte[] raw)
		{
			var test = BitConverter.ToInt32(raw, 4);
			if (test < 0 || test > 100000)
			{
				test++;
			}
			return new HeaderDefinitionMetadata()
			{
				HeaderStartLocationOffsetBytes = BitConverter.ToInt32(raw, 0),
				HeaderSizeInBytes = BitConverter.ToInt32(raw, 4),
				HasUndispatchedCommits = BitConverter.ToBoolean(raw, 8),
			};
		}

		private static void WriteToMs(MemoryStream ms, byte[] data)
		{ ms.Write(data, 0, data.Length); }
	}
}
