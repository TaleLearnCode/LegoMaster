using Newtonsoft.Json;
using System.Linq;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.Infrastructure.Gremlin.Vertices
{
	internal class CategoryVertex : GremlinObjectBase
	{

		/// <summary>
		/// Gets or sets the properties for the vertex.
		/// </summary>
		/// <value>
		/// A <see cref="CategoryProperties"/> representing the properties for the Category vertex.
		/// </value>
		[JsonProperty(propertyName: "properties")]
		internal CategoryProperties Properties { get; set; }

		internal static Category DeserializeAsCategory(string json)
		{
			CategoryVertex vertex = JsonConvert.DeserializeObject<CategoryVertex>(json);
			if (vertex.Type == GremlinLabels.Vertex && vertex.Label == GremlinLabels.Category)
				return new Category()
				{
					Id = vertex.Id,
					UserId = (vertex.Properties != null && vertex.Properties.UserId.Any()) ? vertex.Properties.UserId[0].Value : default,
					Name = (vertex.Properties != null && vertex.Properties.Name.Any()) ? vertex.Properties.Name[0].Value : default,
					RebrickableId = (vertex.Properties != null && vertex.Properties.RebrickableId.Any()) ? vertex.Properties.RebrickableId[0].Value : default
				};
			else
				return default;
		}

	}

	internal class CategoryProperties : VertexPropertyBase { }

}