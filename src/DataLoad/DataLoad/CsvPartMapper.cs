using TinyCsvParser.Mapping;

namespace DataLoad
{
	public class CsvPartMapping : CsvMapping<Part>
	{

		public CsvPartMapping()
		{
			MapProperty(0, x => x.PartNumber);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.CategoryId);
			MapProperty(3, x => x.PartMaterial);
		}

	}
}