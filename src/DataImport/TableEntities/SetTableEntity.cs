using Azure;
using Azure.Data.Tables;
using System;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.DataImport.TableEntities
{

	public class SetTableEntity : Set, ITableEntity
	{
		public string PartitionKey { get; set; }
		public string RowKey { get; set; }
		public DateTimeOffset? Timestamp { get; set; }
		public ETag ETag { get; set; }

		public SetTableEntity() { }

		public SetTableEntity(Set set)
		{
			PartitionKey = "set";
			RowKey = set.SetNumber;
			Id = set.Id;
			SetNumber = set.SetNumber;
			Name = set.Name;
			Year = set.Year;
			ThemeId = set.ThemeId;
			ThemeName = set.ThemeName;
			PartCount = set.PartCount;
		}
	}

}