
using Newtonsoft.Json;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin
{

	/// <summary>
	/// Represents a Gremlin property value.
	/// </summary>
	public class GremlinProperty
	{

		/// <summary>
		/// Gets or sets the identifier of the property value.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the property value identifier.
		/// </value>
		[JsonProperty(propertyName: "id")]
		public string Id { get; set; }

		/// <summary>
		/// Gets or sets the value of the property.
		/// </summary>
		/// <value>
		/// A <c>string</c> representing the property value.
		/// </value>
		[JsonProperty(propertyName: "value")]
		public string Value { get; set; }

	}

}