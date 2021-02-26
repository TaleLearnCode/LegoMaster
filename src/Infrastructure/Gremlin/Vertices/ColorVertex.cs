using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{

	internal class ColorVertex : GremlinObjectBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.Properties)]
		internal ColorProperties Properties { get; set; }

		internal static Color DeserializeAsColor(string json)
		{
			ColorVertex vertex = JsonConvert.DeserializeObject<ColorVertex>(json);
			if (vertex.Type == GremlinLabels.Vertex && vertex.Label == GremlinLabels.Color)
				return new Color()
				{
					Id = vertex.Id,
					UserId = (vertex.Properties != null && vertex.Properties.UserId.Any()) ? vertex.Properties.UserId[0].Value : default,
					Name = (vertex.Properties != null && vertex.Properties.Name.Any()) ? vertex.Properties.Name[0].Value : default,
					RebrickableId = (vertex.Properties != null && vertex.Properties.RebrickableId.Any()) ? vertex.Properties.RebrickableId[0].Value : default,
					RGB = (vertex.Properties != null && vertex.Properties.RGB.Any()) ? vertex.Properties.RGB[0].Value : default,
					IsTranslucent = (vertex.Properties != null && vertex.Properties.IsTranslucent.Any()) && Convert.ToBoolean(vertex.Properties.IsTranslucent[0].Value)
				};
			else
				return default;
		}

	}

	internal class ColorProperties : VertexPropertyBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.RGB)]
		internal List<GremlinProperty> RGB { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.IsTranslucent)]
		internal List<GremlinProperty> IsTranslucent { get; set; }
	}


}