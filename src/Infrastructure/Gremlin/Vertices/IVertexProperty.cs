using System.Collections.Generic;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{

	internal interface IVertexProperty
	{

		internal List<GremlinProperty> UserId { get; set; }

		internal List<GremlinProperty> RebrickableId { get; set; }

		internal List<GremlinProperty> Name { get; set; }

	}

}