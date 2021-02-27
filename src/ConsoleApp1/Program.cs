using System;
using System.Collections.Generic;
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

			TestDictionaryUseCase(gremlinHandler);

			gremlinHandler.Dispose();
		}

		static void TestFirstOrDefault(GremlinHandler gremlinHandler)
		{
			//GremlinObjectBase vertex = gremlinHandler.GetFirstOrDefault<GremlinObjectBase>("g.V().has('setNumber', '10264-1')");
			Category vertex = gremlinHandler.GetFirstOrDefault<Category>("g.V().hasLabel('category').has('rebrickableId', '1')");
			//Color vertex = gremlinHandler.GetFirstOrDefault<Color>("g.V('57fc1784-ce95-4829-bd21-2b37ba4c0c5c')");
			//Color vertex = gremlinHandler.GetFirstOrDefault<Color>("g.V('63acea82-3892-4524-94f8-be12de588f66')");
			//Part vertex = gremlinHandler.GetFirstOrDefault<Part>("g.V('24362868-c754-4aa0-980f-a7f5aa83e044').hasLabel('part')");
			//Set vertex = gremlinHandler.GetFirstOrDefault<Set>("g.V().hasLabel('set').has('setNumber', '10246-1')");
			//Theme vertex = gremlinHandler.GetFirstOrDefault<Theme>("g.V().hasLabel('theme').has('rebrickableId', '227')");

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

		}

		static void TestTypedList(GremlinHandler gremlinHandler)
		{
			List<Color> colors = gremlinHandler.GetList<Color>("g.V().hasLabel('color').has('isTranslucent', 'True')");
			foreach (Color color in colors)
				Console.WriteLine(color.Name);
			Console.WriteLine();
			Console.WriteLine($"Returned {colors.Count} colors");

		}

		static void TestUntypedList(GremlinHandler gremlinHandler)
		{
			List<IEntity> searchResults = gremlinHandler.GetList("g.V().or(has('name', containing('1026')),has('setNumber', containing('1026')),has('partNumber', containing('1026')))");
			ConsoleColor foregroundColor = Console.ForegroundColor;
			foreach (IEntity searchResult in searchResults)
				switch (searchResult.Discriminator)
				{
					case Discriminators.Part:
						Console.ForegroundColor = ConsoleColor.Red;
						Part part = (Part)searchResult;
						Console.WriteLine($"{part.PartNumber}\t{part.Name}");
						break;
					case Discriminators.Set:
						Console.ForegroundColor = ConsoleColor.Yellow;
						Set set = (Set)searchResult;
						Console.WriteLine($"{set.SetNumber}\t{set.Name}");
						break;
				}
			Console.ForegroundColor = foregroundColor;
			Console.WriteLine();
			Console.WriteLine($"Returned {searchResults.Count} colors");

		}

		static void TestDictionary(GremlinHandler gremlinHandler)
		{
			Dictionary<string, List<IEntity>> searchResults = gremlinHandler.GetDictionary("g.V().or(has('name', containing('1026')),has('setNumber', containing('1026')),has('partNumber', containing('1026')))");
			ConsoleColor foregroundColor = Console.ForegroundColor;
			int returnedEntities = 0;
			foreach (KeyValuePair<string, List<IEntity>> searchResult in searchResults)
			{
				switch (searchResult.Key)
				{
					case Discriminators.Part:
						Console.ForegroundColor = ConsoleColor.Red;
						foreach (IEntity entity in searchResult.Value)
						{
							Part part = (Part)entity;
							Console.WriteLine($"{part.PartNumber}\t{part.Name}");
							returnedEntities++;
						}
						break;
					case Discriminators.Set:
						Console.ForegroundColor = ConsoleColor.Yellow;
						foreach (IEntity entity in searchResult.Value)
						{
							Set set = (Set)entity;
							Console.WriteLine($"{set.SetNumber}\t{set.Name}");
							returnedEntities++;
						}
						break;
				}

			}

			Console.ForegroundColor = foregroundColor;
			Console.WriteLine();
			Console.WriteLine($"Returned {searchResults.Count} entity types and {returnedEntities} entities");

		}

		static void TestDictionaryUseCase(GremlinHandler gremlinHandler)
		{
			Dictionary<string, List<IEntity>> searchResults = gremlinHandler.GetDictionary("g.V().or(has('name', containing('1026')),has('setNumber', containing('1026')),has('partNumber', containing('1026')))");
			Console.WriteLine($"Sets Returned: {searchResults[Discriminators.Set].Count}");
			Console.WriteLine($"Parts Returned: {searchResults[Discriminators.Part].Count}");

		}

	}
}
