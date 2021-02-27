namespace TaleLearnCode.LEGOMaster.Domain
{

	public class Category : IEntity
	{
		public string Id { get; set; }

		public string UserId { get; set; }

		public string Discriminator => Discriminators.Category;

		public string Name { get; set; }

		public string RebrickableId { get; set; }

	}

}