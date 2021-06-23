using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RawRabbit.Exceptions;
using RawRabbit.Logging;

namespace RawRabbit.Channel
{
	public interface IChannelPool
	{
		Task<IModel> GetAsync(CancellationToken ct = default(CancellationToken));
	}

	public class StaticChannelPool : IDisposable, IChannelPool
	{
		protected readonly LinkedList<IModel> Pool;
		protected readonly List<IRecoverable> Recoverables;
		protected readonly ConcurrentChannelQueue ChannelRequestQueue;
		protected object ThisLock { get; } = new object();
		private LinkedListNode<IModel> _current;
		private readonly ILog _logger = LogProvider.For<StaticChannelPool>();

		public StaticChannelPool(IEnumerable<IModel> seed)
		{
			seed = seed.ToList();
			Pool = new LinkedList<IModel>(seed);
			Recoverables = new List<IRecoverable>();
			ChannelRequestQueue = new ConcurrentChannelQueue();
			ChannelRequestQueue.Queued += (sender, args) => StartServeChannels();
			foreach (var channel in seed)
			{
				ConfigureRecovery(channel);
			}
		}

		private void StartServeChannels()
		{
			lock (ThisLock)
			{
				try
				{
					if (ChannelRequestQueue.IsEmpty || Pool.Count == 0)
					{
						_logger.Debug("Unable to serve channels. The pool consists of {channelCount} channels and {channelRequests} requests for channels.", Pool.Count, ChannelRequestQueue.Count);
						return;
					}

					_logger.Debug("Starting serving channels.");
					do
					{
						_current = _current?.Next ?? Pool.First;
						if (_current == null)
						{
							_logger.Debug("Unable to serve channels. Pool empty.");
							return;
						}

						if (_current.Value.IsClosed)
						{
							Pool.Remove(_current);
							if (Pool.Count != 0)
							{
								continue;
							}

							if (Recoverables.Count == 0)
							{
								throw new ChannelAvailabilityException("No open channels in pool and no recoverable channels");
							}

							_logger.Info("No open channels in pool, but {recoveryCount} waiting for recovery", Recoverables.Count);
							return;
						}

						if (ChannelRequestQueue.TryDequeue(out var cTsc))
						{
							cTsc.TrySetResult(_current.Value);
						}
					} while (!ChannelRequestQueue.IsEmpty);
				}
				catch (Exception e)
				{
					_logger.Info(e, "An unhandled exception occurred when serving channels.");
					throw;
				}
			}
		}

		protected virtual int GetActiveChannelCount()
		{
			lock (ThisLock)
			{
				return Enumerable
					.Concat<object>(Pool, Recoverables)
					.Distinct()
					.Count();
			}
		}

		protected void ConfigureRecovery(IModel channel)
		{
			if (!(channel is IRecoverable recoverable))
			{
				_logger.Debug("Channel {channelNumber} is not recoverable. Recovery disabled for this channel.", channel.ChannelNumber);
				return;
			}
			if (channel.IsClosed && channel.CloseReason != null && channel.CloseReason.Initiator == ShutdownInitiator.Application)
			{
				_logger.Debug("{Channel {channelNumber} is closed by the application. Channel will remain closed and not be part of the channel pool", channel.ChannelNumber);
				return;
			}
			lock (ThisLock)
			{
				Recoverables.Add(recoverable);
			}
			recoverable.Recovery += (sender, args) =>
			{
				_logger.Info("Channel {channelNumber} has been recovered and will be re-added to the channel pool", channel.ChannelNumber);
				lock (ThisLock)
				{
					if (Pool.Contains(channel))
					{
						return;
					}
					Pool.AddLast(channel);
				}
				StartServeChannels();
			};
			channel.ModelShutdown += (sender, args) =>
			{
				if (args.Initiator == ShutdownInitiator.Application)
				{
					_logger.Info("Channel {channelNumber} is being closed by the application. No recovery will be performed.", channel.ChannelNumber);
					lock (ThisLock)
					{
						Recoverables.Remove(recoverable);
					}
				}
			};
		}

		public virtual Task<IModel> GetAsync(CancellationToken ct = default(CancellationToken))
		{
			var channelTcs = ChannelRequestQueue.Enqueue();
			ct.Register(() => channelTcs.TrySetCanceled());
			return channelTcs.Task;
		}

		public virtual void Dispose()
		{
			foreach (var channel in Pool)
			{
				channel?.Dispose();
			}
			foreach (var recoverable in Recoverables)
			{
				(recoverable as IModel)?.Dispose();
			}
		}
	}
}
