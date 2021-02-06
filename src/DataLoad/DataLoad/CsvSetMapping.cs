using TinyCsvParser.Mapping;

namespace DataLoad
{
	public class CsvSetMapping : CsvMapping<Set>
	{

		public CsvSetMapping()
		{
			MapProperty(0, x => x.SetNumber);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.Year);
			MapProperty(3, x => x.ThemeId);
			MapProperty(4, x => x.PartCount);
		}

	}
}
