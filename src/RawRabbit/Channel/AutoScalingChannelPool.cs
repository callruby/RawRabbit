using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RawRabbit.Channel.Abstraction;
using RawRabbit.Logging;

namespace RawRabbit.Channel
{
	public class AutoScalingChannelPool : DynamicChannelPool
	{
		private readonly IChannelFactory _factory;
		private readonly AutoScalingOptions _options;
		private Timer _timer;
		private readonly ILog _logger = LogProvider.For<AutoScalingChannelPool>();
		private readonly SemaphoreSlim _addingChannels = new SemaphoreSlim(1, 1);

		public AutoScalingChannelPool(IChannelFactory factory, AutoScalingOptions options)
		{
			_factory = factory;
			_options = options;
			ValidateOptions(options);
			SetupScaling();
		}

		private static void ValidateOptions(AutoScalingOptions options)
		{
			if (options.MinimunPoolSize <= 0)
			{
				throw new ArgumentException($"Minimum Pool Size needs to be a positive integer. Got: {options.MinimunPoolSize}");
			}
			if (options.MaximumPoolSize <= 0)
			{
				throw new ArgumentException($"Maximum Pool Size needs to be a positive integer. Got: {options.MinimunPoolSize}");
			}
			if (options.MinimunPoolSize > options.MaximumPoolSize)
			{
				throw new ArgumentException($"The Maximum Pool Size ({options.MaximumPoolSize}) must be larger than the Minimum Pool Size ({options.MinimunPoolSize})");
			}
		}

		public override async Task<IModel> GetAsync(CancellationToken ct = default(CancellationToken))
		{
			if (GetActiveChannelCount() < _options.MinimunPoolSize)
			{
				await _addingChannels.WaitAsync(ct);
				try
				{
					var count = GetActiveChannelCount();
					if (count < _options.MinimunPoolSize)
					{
						_logger.Debug("Pool currently has {channelCount}, which is lower than the minimal pool size {minimalPoolSize}. Creating channels.", count, _options.MinimunPoolSize);
						var tasks = new List<Task<IModel>>(_options.MinimunPoolSize - count);
						do
						{
							tasks.Add(_factory.CreateChannelAsync(ct));
						} while (++count < _options.MinimunPoolSize);

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

		public void SetupScaling()
		{
			if (_options.RefreshInterval == TimeSpan.MaxValue || _options.RefreshInterval == TimeSpan.MinValue)
			{
				return;
			}

			_timer = new Timer(state =>
			{
				int poolCount;
				int requestCount;
				lock (ThisLock)
				{
					poolCount = Pool.Count;
					requestCount = ChannelRequestQueue.Count;
				}

				var workPerChannel = poolCount == 0 ? int.MaxValue : requestCount / poolCount;
				var scaleUp = poolCount < _options.MaximumPoolSize;
				var scaleDown = _options.MinimunPoolSize < poolCount;

				_logger.Debug("Channel pool currently has {channelCount} channels open and a total workload of {totalWorkload}", poolCount, requestCount);
				if (scaleUp && _options.DesiredAverageWorkload < workPerChannel)
				{
					_logger.Debug("The estimated workload is {averageWorkload} operations/channel, which is higher than the desired workload ({desiredAverageWorkload}). Creating channel.", workPerChannel, _options.DesiredAverageWorkload);

					var channelCancellation = new CancellationTokenSource(_options.RefreshInterval);
					_factory
						.CreateChannelAsync(channelCancellation.Token)
						.ContinueWith(tChannel =>
						{
							if (tChannel.Status == TaskStatus.RanToCompletion)
							{
								Add(tChannel.Result);
							}
						}, CancellationToken.None);
					return;
				}

				if (scaleDown && workPerChannel < _options.DesiredAverageWorkload)
				{
					_logger.Debug("The estimated workload is {averageWorkload} operations/channel, which is lower than the desired workload ({desiredAverageWorkload}). Deleting channel.", workPerChannel, _options.DesiredAverageWorkload);
					IModel toRemove;
					lock (ThisLock)
					{
						toRemove = Pool.FirstOrDefault();
						Remove(toRemove);
					}
					Timer disposeTimer = null;
					disposeTimer = new Timer(o =>
					{
						(o as IModel)?.Dispose();
						disposeTimer?.Dispose();
					}, toRemove, _options.GracefulCloseInterval, new TimeSpan(-1));
				}
			}, null, _options.RefreshInterval, _options.RefreshInterval);
		}

		public override void Dispose()
		{
			base.Dispose();
			_timer?.Dispose();
		}
	}

	public class AutoScalingOptions
	{
		public int DesiredAverageWorkload { get; set; }
		public int MinimunPoolSize { get; set; }
		public int MaximumPoolSize { get; set; }
		public TimeSpan RefreshInterval { get; set; }
		public TimeSpan GracefulCloseInterval { get; set; }

		public static AutoScalingOptions Default => new AutoScalingOptions
		{
			MinimunPoolSize = 1,
			MaximumPoolSize = 10,
			DesiredAverageWorkload = 20000,
			RefreshInterval = TimeSpan.FromSeconds(10),
			GracefulCloseInterval = TimeSpan.FromSeconds(30)
		};
	}
}
