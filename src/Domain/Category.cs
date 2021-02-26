namespace TaleLearnCode.LEGOMaster.Domain
{

	public abstract class BaseObject { }

	public class Category : BaseObject
	{
		public string Id { get; set; }

		public string UserId { get; set; }

		public string Name { get; set; }

		public string RebrickableId { get; set; }

	}

}