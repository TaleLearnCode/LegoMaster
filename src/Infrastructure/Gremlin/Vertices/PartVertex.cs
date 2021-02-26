using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{
	
	internal class PartVertex : GremlinObjectBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.Properties)]
		internal PartProperties Properties { get; set; }

		internal static Part DeserializeAsPart(string json)
		{
			PartVertex vertex = JsonConvert.DeserializeObject<PartVertex>(json);
			if (vertex.Type == GremlinLabels.Vertex && vertex.Label == GremlinLabels.Part)
				return new Part()
				{
					Id = vertex.Id,
					UserId = (vertex.Properties != null && vertex.Properties.UserId != null && vertex.Properties.UserId.Any()) ? vertex.Properties.UserId[0].Value : default,
					Name = (vertex.Properties != null && vertex.Properties.Name != null && vertex.Properties.Name.Any()) ? vertex.Properties.Name[0].Value : default,
					PartNumber = (vertex.Properties != null && vertex.Properties.PartNubmer != null && vertex.Properties.PartNubmer.Any()) ? vertex.Properties.PartNubmer[0].Value : default,
					CategoryId = (vertex.Properties != null && vertex.Properties.CategoryId != null && vertex.Properties.CategoryId.Any()) ? vertex.Properties.CategoryId[0].Value : default,
					CategoryName = (vertex.Properties != null && vertex.Properties.CategoryName != null && vertex.Properties.CategoryName.Any()) ? vertex.Properties.CategoryName[0].Value : default,
					PartMaterial = (vertex.Properties != null && vertex.Properties.PartMaterial != null && vertex.Properties.PartMaterial.Any()) ? vertex.Properties.PartMaterial[0].Value : default
				};
			else
				return default;
		}

	}

	internal class PartProperties : VertexPropertyBase
	{

		[JsonProperty(propertyName: GremlinPropertyNames.PartNumber)]
		internal List<GremlinProperty> PartNubmer { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.CategoryId)]
		internal List<GremlinProperty> CategoryId { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.CategoryName)]
		internal List<GremlinProperty> CategoryName { get; set; }

		[JsonProperty(propertyName: GremlinPropertyNames.PartMaterial)]
		internal List<GremlinProperty> PartMaterial { get; set; }

	}

}