use axiom::prelude::*;
use criterion::*;
use std::time::Duration;
use std::time::Instant;

/// Spawns an actor with an (index, max_index), that automatically shutdowns the system
/// when the actor index == max_index.
async fn spawner(
    (index, max_index): (usize, usize),
    ctx: Context,
    _: Message,
) -> ActorResult<(usize, usize)> {
    if index == max_index - 1 {
        ctx.system.trigger_shutdown();
    }

    Ok(Status::done((index, max_index)))
}

/// Creates a node in a RingBuffer, it
/// 1. Takes a usize message.
/// 2. Increments it by one.
/// 3. Sends it to the `next` actor.
async fn ring_node(
    (limit, next): (usize, Aid),
    _: Context,
    message: Message,
) -> ActorResult<(usize, Aid)> {
    if let Some(current) = message.content_as::<usize>() {
        next.send_new(*current + 1).unwrap();
    }

    Ok(Status::done((limit, next)))
}

struct RingNodeRoot {
    nodes: usize,
    limit: usize,
    next: Option<Aid>,
}

impl RingNodeRoot {
    /// Creates the root ring node, where the ring buffer will contain
    /// `nodes` determine how many actors are in the ring.
    /// `messages` determine how many times a message will go around the ring.
    fn new(nodes: usize, messages: usize) -> RingNodeRoot {
        RingNodeRoot {
            nodes,
            limit: nodes * messages,
            next: None,
        }
    }

    /// This RingNodeRoot actor is responsible for spawning all the actors
    /// in the ring buffer, and associating each actor with the next actor
    /// in the sequence.
    ///
    /// Once all actors spawn, it sends the first message into the ring buffer.
    pub async fn handle(mut self, ctx: Context, message: Message) -> ActorResult<Self> {
        if let Some(sys_msg) = message.content_as::<SystemMsg>() {
            match &*sys_msg {
                SystemMsg::Start => {
                    let aid = (0..self.nodes).fold(ctx.aid.clone(), |previous_aid, _| {
                        ctx.system
                            .spawn()
                            .with((self.limit, previous_aid), ring_node)
                            .unwrap()
                    });

                    self.next = Some(aid.clone());

                    return Ok(Status::done(self));
                }
                _ => {}
            }
        }

        if let Some(current) = message.content_as::<usize>() {
            let aid = self.next.clone();

            if *current >= self.limit - 1 {
                ctx.system.trigger_shutdown();
            } else {
                aid.unwrap().send_new(*current + 1).unwrap();
            }
        }

        Ok(Status::done(self))
    }
}

fn bench_spawn(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn");

    group.sample_size(30);

    group.bench_function("100 actors", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();

            for _ in 0..iters {
                // Create the ActorSystem
                let config = ActorSystemConfig::default();
                let system = ActorSystem::create(config);

                // Spawn 100 actors.
                for i in 0..100 {
                    let _ = system.spawn().with((i, 100), spawner).unwrap();
                }

                // Wait for system to shutdown, and ideally all threads should be terminated.
                system.await_shutdown(None);
            }

            start.elapsed()
        })
    });
}

fn bench_ring_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer");

    group.sample_size(20);
    group.measurement_time(Duration::from_secs(50));

    for (nodes, messages) in [(10, 10), (100, 100)].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", (nodes, messages))),
            &(nodes, messages),
            |bench, (nodes, messages)| {
                bench.iter_custom(|iters| {
                    let start = Instant::now();

                    for _ in 0..iters {
                        black_box({
                            let config = ActorSystemConfig::default();
                            let system = ActorSystem::create(config);

                            let root = system
                                .spawn()
                                .with(
                                    RingNodeRoot::new(**nodes as usize, **messages as usize),
                                    RingNodeRoot::handle,
                                )
                                .unwrap();

                            root.send_new(0 as usize).unwrap();

                            system.await_shutdown(None);
                        });
                    }

                    start.elapsed()
                })
            },
        );
    }
}

criterion_group!(basic, bench_spawn, bench_ring_buffer);

criterion_main!(basic);
