using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{

	internal class SetVertex : GremlinObjectBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.Properties)]
		internal SetProperties Properties { get; set; }

		internal static Set DeserializeAsSet(string json)
		{
			SetVertex vertex = JsonConvert.DeserializeObject<SetVertex>(json);
			if (vertex.Type == GremlinLabels.Vertex && vertex.Label == GremlinLabels.Set)
			{
				return new Set()
				{
					Id = vertex.Id,
					UserId = (vertex.Properties != null && vertex.Properties.UserId != null && vertex.Properties.UserId.Any()) ? vertex.Properties.UserId[0].Value : default,
					Name = (vertex.Properties != null && vertex.Properties.Name != null && vertex.Properties.Name.Any()) ? vertex.Properties.Name[0].Value : default,
					SetNumber = (vertex.Properties != null && vertex.Properties.SetNumber != null && vertex.Properties.SetNumber.Any()) ? vertex.Properties.SetNumber[0].Value : default,
					Year = (vertex.Properties != null && vertex.Properties.Year != null && vertex.Properties.Year.Any() && int.TryParse(vertex.Properties.Year[0].Value, out int year)) ? year : default,
					ThemeId = (vertex.Properties != null && vertex.Properties.ThemeId != null && vertex.Properties.ThemeId.Any()) ? vertex.Properties.ThemeId[0].Value : default,
					ThemeName = (vertex.Properties != null && vertex.Properties.ThemeName != null && vertex.Properties.ThemeName.Any()) ? vertex.Properties.ThemeName[0].Value : default,
					PartCount = (vertex.Properties != null && vertex.Properties.PartCount != null && vertex.Properties.PartCount.Any() && int.TryParse(vertex.Properties.PartCount[0].Value, out int partCount)) ? partCount : default,
				};
			}
			else
				return default;
		}

	}

	internal class SetProperties : VertexPropertyBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.SetNumber)]
		internal List<GremlinProperty> SetNumber { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.Year)]
		internal List<GremlinProperty> Year { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.ThemeId)]
		internal List<GremlinProperty> ThemeId { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.ThemeName)]
		internal List<GremlinProperty> ThemeName { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.PartCount)]
		internal List<GremlinProperty> PartCount { get; set; }

	}

}