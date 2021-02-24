using Azure;
using Azure.Data.Tables;
using System;
using TaleLearnCode.LEGOMaster.Domain;

namespace TaleLearnCode.LEGOMaster.DataImport.TableEntities
{

	public class ThemeTableEntity : Theme, ITableEntity
	{

		public string PartitionKey { get; set; }
		public string RowKey { get; set; }
		public DateTimeOffset? Timestamp { get; set; }
		public ETag ETag { get; set; }

		public ThemeTableEntity() { }

		public ThemeTableEntity(Theme theme)
		{
			PartitionKey = "theme";
			RowKey = theme.RebrickableId;
			Id = theme.Id;
			Name = theme.Name;
			RebrickableId = theme.RebrickableId;
			ParentId = theme.ParentId;
			ParentName = theme.ParentName;
		}

		public bool EqualsTheme(Theme theme)
		{
			if (
				theme.Id != Id
				|| theme.Name != Name
				|| theme.RebrickableId != RebrickableId
				|| theme.ParentId != ParentId
				|| theme.ParentName != ParentName)
				return false;
			else
				return true;
		}

	}

}