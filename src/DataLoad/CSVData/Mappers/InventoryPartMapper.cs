﻿using System;
using TaleLearnCode.LegoMaster.CSVData.Models;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace TaleLearnCode.LegoMaster.CSVData.Mappers
{
	public class InventoryPartMapper : CsvMapping<InventoryPart>
	{

		public InventoryPartMapper()
		{
			MapProperty(0, x => x.InventoryId);
			MapProperty(1, x => x.PartNumber);
			MapProperty(2, x => x.ColorId);
			MapProperty(3, x => x.Quantity);
			MapProperty(4, x => x.IsSpare, new BoolConverter("t", "f", StringComparison.InvariantCulture));
		}

	}
}