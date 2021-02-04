using System;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace DataLoad
{

	public class CsvColorMapping : CsvMapping<Color>
	{

		public CsvColorMapping() : base()
		{
			MapProperty(0, x => x.Id);
			MapProperty(1, x => x.Name);
			MapProperty(2, x => x.RGB);
			MapProperty(3, x => x.IsTranslucent, new BoolConverter("1", "0", StringComparison.InvariantCulture));
		}

	}

}
