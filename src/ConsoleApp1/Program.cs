using System;
using System.Reflection;
using TaleLearnCode.LEGOMaster.DataImport;
using TaleLearnCode.LEGOMaster.Domain;
using TaleLearnCode.LEGOMaster.Infrastructure.Gremlin;

namespace ConsoleApp1
{
	class Program
	{
		static void Main()
		{
			GremlinSettings gremlinSettings = new()
			{
				DatabaseName = Settings.Database,
				AccountKey = Settings.PrimaryKey,
				ContainerName = Settings.Container,
				EnableSSL = Settings.EnableSSL,
				Host = Settings.Host,
				Port = Settings.Port
			};

			GremlinHandler gremlinHandler = new(gremlinSettings);
			//GremlinObjectBase vertex = gremlinHandler.GetFirstOrDefault<GremlinObjectBase>("g.V().has('setNumber', '10264-1')");
			//Category vertex = gremlinHandler.GetFirstOrDefault<Category>("g.V().hasLabel('category').has('rebrickableId', '1')");
			//Color vertex = gremlinHandler.GetFirstOrDefault<Color>("g.V('57fc1784-ce95-4829-bd21-2b37ba4c0c5c')");
			//Color vertex = gremlinHandler.GetFirstOrDefault<Color>("g.V('63acea82-3892-4524-94f8-be12de588f66')");
			//Part vertex = gremlinHandler.GetFirstOrDefault<Part>("g.V('24362868-c754-4aa0-980f-a7f5aa83e044').hasLabel('part')");
			//Set vertex = gremlinHandler.GetFirstOrDefault<Set>("g.V().hasLabel('set').has('setNumber', '10246-1')");
			Theme vertex = gremlinHandler.GetFirstOrDefault<Theme>("g.V().hasLabel('theme').has('rebrickableId', '227')");


			ConsoleColor foregroundColor = Console.ForegroundColor;
			PropertyInfo[] vertexPropertyInfo = vertex.GetType().GetProperties();
			int maxPropertyNameLength = 0;
			foreach (PropertyInfo propertyInfo in vertexPropertyInfo)
				if (propertyInfo.Name.Length > maxPropertyNameLength) maxPropertyNameLength = propertyInfo.Name.Length;
			foreach (PropertyInfo propertyInfo in vertexPropertyInfo)
			{
				if (propertyInfo.CanRead)
				{
					Console.ForegroundColor = ConsoleColor.Green;
					Console.Write($"{propertyInfo.Name.PadRight(maxPropertyNameLength)} : ");
					Console.ForegroundColor = foregroundColor;
					Console.Write($"{vertex.GetType().GetProperty(propertyInfo.Name).GetValue(vertex, default)}\n");
				}
			}


			gremlinHandler.Dispose();
		}
	}
}
