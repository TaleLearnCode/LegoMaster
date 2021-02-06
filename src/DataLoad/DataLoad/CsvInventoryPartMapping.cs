using System;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace DataLoad
{
	public class CsvInventoryPartMapping : CsvMapping<InventoryPart>
	{

		public CsvInventoryPartMapping()
		{
			MapProperty(0, x => x.InventoryId);
			MapProperty(1, x => x.PartNumber);
			MapProperty(2, x => x.ColorId);
			MapProperty(3, x => x.Quantity);
			MapProperty(4, x => x.IsSpare, new BoolConverter("t", "f", StringComparison.InvariantCulture));
		}

	}
}