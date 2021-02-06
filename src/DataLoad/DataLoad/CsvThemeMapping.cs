using TinyCsvParser.Mapping;

namespace DataLoad
{
	public class CsvThemeMapping : CsvMapping<Theme>
	{

		public CsvThemeMapping()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.ParentId);
		}

	}
}