namespace TaleLearnCode.LEGOMaster.Domain
{
	public interface IEntity
	{

		public string Id { get; set; }

		public string UserId { get; set; }

		public string Discriminator { get; }

	}
}
