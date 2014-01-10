using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AlphaTester
{
	[Serializable]
	public class FooChangedEvent
	{
		public int NewFoo
		{ get; set; }

		public byte[] NewFooKb
		{ get; set; }

		public FooChangedEvent( int newFoo )
		{
			NewFoo = newFoo;
			NewFooKb = new byte[newFoo];
		}
	}
}
