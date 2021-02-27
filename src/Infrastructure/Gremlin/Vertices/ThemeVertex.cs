using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{
	internal class ThemeVertex : GremlinObjectBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.Properties)]
		internal ThemeProperties Properties { get; set; }

		internal static Theme DeserializeAsTheme(string json)
		{
			ThemeVertex vertex = JsonConvert.DeserializeObject<ThemeVertex>(json);
			if (vertex.Type == GremlinLabels.Vertex && vertex.Label == GremlinLabels.Theme)
				return new Theme()
				{
					Id = vertex.Id,
					UserId = (vertex.Properties != null && vertex.Properties.UserId != null && vertex.Properties.UserId.Any()) ? vertex.Properties.UserId[0].Value : default,
					Name = (vertex.Properties != null && vertex.Properties.Name != null && vertex.Properties.Name.Any()) ? vertex.Properties.Name[0].Value : default,
					RebrickableId = (vertex.Properties != null && vertex.Properties.RebrickableId != null && vertex.Properties.RebrickableId.Any()) ? vertex.Properties.RebrickableId[0].Value : default,
					ParentId = (vertex.Properties != null && vertex.Properties.ParentId != null && vertex.Properties.ParentId.Any()) ? vertex.Properties.ParentId[0].Value : default,
					ParentName = (vertex.Properties != null && vertex.Properties.ParentName != null && vertex.Properties.ParentName.Any()) ? vertex.Properties.ParentName[0].Value : default
				};
			else
				return default;
		}

	}

	internal class ThemeProperties : VertexPropertyBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.ParentId)]
		internal List<GremlinProperty> ParentId { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.ParentName)]
		internal List<GremlinProperty> ParentName { get; set; }

	}
}
