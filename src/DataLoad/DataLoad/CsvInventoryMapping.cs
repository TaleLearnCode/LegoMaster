using TinyCsvParser.Mapping;

namespace DataLoad
{
	public class CsvInventoryMapping : CsvMapping<Inventory>
	{

		public CsvInventoryMapping()
		{
			MapProperty(0, x => x.Id);
			MapProperty(1, x => x.Version);
			MapProperty(2, x => x.SetNumber);
		}

	}
}