using TaleLearnCode.LEGOMaster.Domain;
using TinyCsvParser.Mapping;

namespace TaleLearnCode.LEGOMaster.DataImport
{
	public class ThemeMapper : CsvMapping<Theme>
	{

		public ThemeMapper()
		{
			MapProperty(0, x => x.RebrickableId);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.ParentId);
		}

	}
}