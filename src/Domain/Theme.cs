namespace TaleLearnCode.LEGOMaster.Domain
{
	public class Theme : IEntity
	{

		public string Id { get; set; }

		public string UserId { get; set; }

		public string Discriminator => Discriminators.Theme;

		public string RebrickableId { get; set; }

		public string Name { get; set; }

		public string ParentId { get; set; }

		public string ParentName { get; set; }

	}

}