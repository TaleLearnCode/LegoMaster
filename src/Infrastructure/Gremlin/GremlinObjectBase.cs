using System.Text.Json.Serialization;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin
{

	/// <summary>
	/// Base type for types representing Gremlin objects.
	/// </summary>
	public class GremlinObjectBase
	{

		[JsonPropertyName("id")]
		public string Id { get; set; }

		[JsonPropertyName("label")]
		public string Label { get; set; }

		[JsonPropertyName("type")]
		public string Type { get; set; }

	}

}