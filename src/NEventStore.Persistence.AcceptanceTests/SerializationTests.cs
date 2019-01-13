namespace NEventStore.Serialization.AcceptanceTests
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using FluentAssertions;
	using NEventStore.Persistence.AcceptanceTests;
	using NEventStore.Persistence.AcceptanceTests.BDD;
	using Xunit;

    public class when_serializing_a_simple_message : SerializationConcern
    {
        private readonly SimpleMessage _message = new SimpleMessage().Populate();
        private SimpleMessage _deserialized;
        private byte[] _serialized;

		protected override Task Context()
        {
            _serialized = Serializer.Serialize(_message);
			return Task.FromResult(true);
		}

		protected override Task Because()
        {
            _deserialized = Serializer.Deserialize<SimpleMessage>(_serialized);
			return Task.FromResult(true);
		}

        [Fact]
        public void should_deserialize_a_message_which_contains_the_same_Id_as_the_serialized_message()
        {
            _deserialized.Id.Should().Be(_message.Id);
        }

        [Fact]
        public void should_deserialize_a_message_which_contains_the_same_Value_as_the_serialized_message()
        {
            _deserialized.Value.Should().Be(_message.Value);
        }

        [Fact]
        public void should_deserialize_a_message_which_contains_the_same_Created_value_as_the_serialized_message()
        {
            _deserialized.Created.Should().Be(_message.Created);
        }

        [Fact]
        public void should_deserialize_a_message_which_contains_the_same_Count_as_the_serialized_message()
        {
            _deserialized.Count.Should().Be(_message.Count);
        }

        [Fact]
        public void should_deserialize_a_message_which_contains_the_number_of_elements_as_the_serialized_message()
        {
            _deserialized.Contents.Count.Should().Be(_message.Contents.Count);
        }

        [Fact]
        public void should_deserialize_a_message_which_contains_the_same_Contents_as_the_serialized_message()
        {
            _deserialized.Contents.SequenceEqual(_message.Contents).Should().BeTrue();
        }
    }

    public class when_serializing_a_list_of_event_messages : SerializationConcern
    {
        private readonly List<EventMessage> Messages = new List<EventMessage>
        {
            new EventMessage {Body = "some value"},
            new EventMessage {Body = 42},
            new EventMessage {Body = new SimpleMessage()}
        };

        private List<EventMessage> _deserialized;
        private byte[] _serialized;

		protected override Task Context()
        {
            _serialized = Serializer.Serialize(Messages);
			return Task.FromResult(true);
		}

		protected override Task Because()
        {
            _deserialized = Serializer.Deserialize<List<EventMessage>>(_serialized);
			return Task.FromResult(true);
		}

        [Fact]
        public void should_deserialize_the_same_number_of_event_messages_as_it_serialized()
        {
            Messages.Count.Should().Be(_deserialized.Count);
        }

        [Fact]
        public void should_deserialize_the_the_complex_types_within_the_event_messages()
        {
            _deserialized.Last().Body.Should().BeOfType<SimpleMessage>();
        }
    }

    public class when_serializing_a_list_of_commit_headers : SerializationConcern
    {
        private readonly Dictionary<string, object> _headers = new Dictionary<string, object>
        {
            {"HeaderKey", "SomeValue"},
            {"AnotherKey", 42},
            {"AndAnotherKey", Guid.NewGuid()},
            {"LastKey", new SimpleMessage()}
        };

        private Dictionary<string, object> _deserialized;
        private byte[] _serialized;

		protected override Task Context()
        {
            _serialized = Serializer.Serialize(_headers);
			return Task.FromResult(true);
        }

        protected override Task Because()
        {
            _deserialized = Serializer.Deserialize<Dictionary<string, object>>(_serialized);
			return Task.FromResult(true);
		}

        [Fact]
        public void should_deserialize_the_same_number_of_event_messages_as_it_serialized()
        {
            _headers.Count.Should().Be(_deserialized.Count);
        }

        [Fact]
        public void should_deserialize_the_the_complex_types_within_the_event_messages()
        {
            _deserialized.Last().Value.Should().BeOfType<SimpleMessage>();
        }
    }

    public class when_serializing_an_untyped_payload_on_a_snapshot : SerializationConcern
    {
        private Snapshot _deserialized;
        private IDictionary<string, List<int>> _payload;
        private byte[] _serialized;
        private Snapshot _snapshot;

		protected override Task Context()
        {
            _payload = new Dictionary<string, List<int>>();
            _snapshot = new Snapshot(Guid.NewGuid().ToString(), 42, _payload);
            _serialized = Serializer.Serialize(_snapshot);
			return Task.FromResult(true);
		}

		protected override Task Because()
        {
            _deserialized = Serializer.Deserialize<Snapshot>(_serialized);
			return Task.FromResult(true);
		}

        [Fact]
        public void should_correctly_deserialize_the_untyped_payload_contents()
        {
            _deserialized.Payload.ShouldBeEquivalentTo(_snapshot.Payload);
        }

        [Fact]
        public void should_correctly_deserialize_the_untyped_payload_type()
        {
            _deserialized.Payload.Should().BeOfType(_snapshot.Payload.GetType());
        }
    }

    public class SerializationConcern : SpecificationBase, IUseFixture<SerializerFixture>
    {
        private SerializerFixture _data;

        public ISerialize Serializer
        {
            get { return _data.Serializer; }
        }

        public void SetFixture(SerializerFixture data)
        {
            _data = data;
        }
    }

    public partial class SerializerFixture
    {
        private readonly Func<ISerialize> _createSerializer;
        private ISerialize _serializer;

        public ISerialize Serializer
        {
            get { return _serializer ?? (_serializer = _createSerializer()); }
        }
    }
}