using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RawRabbit.Channel.Abstraction;
using RawRabbit.Logging;

namespace RawRabbit.Channel
{
	public class ResilientChannelPool : DynamicChannelPool
	{
		protected readonly IChannelFactory ChannelFactory;
		private readonly int _desiredChannelCount;
		private readonly ILog _logger = LogProvider.For<ResilientChannelPool>();
		private readonly SemaphoreSlim _addingChannels = new SemaphoreSlim(1, 1);

		public ResilientChannelPool(IChannelFactory factory, int channelCount)
			: this(factory, CreateSeed(factory, channelCount)) { }

		public ResilientChannelPool(IChannelFactory factory)
			: this(factory, Enumerable.Empty<IModel>()) { }

		public ResilientChannelPool(IChannelFactory factory, IEnumerable<IModel> seed) : base(seed)
		{
			ChannelFactory = factory;
			_desiredChannelCount = seed.Count();
		}

		private static IEnumerable<IModel> CreateSeed(IChannelFactory factory, int channelCount)
		{
			for (var i = 0; i < channelCount; i++)
			{
				yield return factory.CreateChannelAsync().GetAwaiter().GetResult();
			}
		}

		public override async Task<IModel> GetAsync(CancellationToken ct = default(CancellationToken))
		{
			if (GetActiveChannelCount() < _desiredChannelCount)
			{
				await _addingChannels.WaitAsync(ct);
				try
				{
					var count = GetActiveChannelCount();
					if (count < _desiredChannelCount)
					{
						_logger.Debug("Pool currently has {channelCount}, which is lower than the desired pool size {minimalPoolSize}. Creating channels.", count, _desiredChannelCount);
						var tasks = new List<Task<IModel>>(_desiredChannelCount - count);
						do
						{
							tasks.Add(ChannelFactory.CreateChannelAsync(ct));
						} while (++count < _desiredChannelCount);

						while (tasks.Any())
						{
							var t = await Task.WhenAny(tasks);
							tasks.Remove(t);
							Add(t.Result);
						}
					}
				}
				finally
				{
					_addingChannels.Release();
				}
			}

			return await base.GetAsync(ct);
		}
	}
}
