using TinyCsvParser.Mapping;

namespace DataLoad
{
	public class CsvCategoryMapping : CsvMapping<Category>
	{

		public CsvCategoryMapping()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
		}

	}
}
