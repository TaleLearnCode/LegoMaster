using Newtonsoft.Json;
using System.Collections.Generic;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{

	internal abstract class VertexPropertyBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.UserId)]
		internal List<GremlinProperty> UserId { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.RebrickableId)]
		internal List<GremlinProperty> RebrickableId { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.Name)]
		internal List<GremlinProperty> Name { get; set; }

	}

}