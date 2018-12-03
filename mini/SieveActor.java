package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import java.util.*;
import java.util.stream.IntStream;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An pcdp actor-based implementation of the Sieve of Eratosthenes.
 */
public final class SieveActor extends Sieve {

    /**
     * {@inheritDoc}
     *
     * Calculating the number of primes <= limit in parallel.
     * Sieve of Eratosthenes as a pipeline of actors, each corresponding to a single prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final CollectorActor collActor = new CollectorActor();
        finish(() -> {
                    final int start = 2;
                    final SieveActorActor first = initActor(start, limit, collActor);
                    IntStream.range(start, limit).forEach(first::send);
                }
        );
        return collActor.size();
    }

    private static SieveActorActor initActor(final int start, final int limit, final CollectorActor collActor) {
        return new SieveActorActor(start, limit, collActor);
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        private int limit;
        private int start;
        private Optional<SieveActorActor> nextActor = Optional.empty();
        private CollectorActor collActor;

        SieveActorActor(final int start, final int limit, final CollectorActor collActor) {
            this.start=start;
            this.limit=limit;
            this.collActor=collActor;
        }

        /**
         * Process a single message sent to this actor.
         *
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            final int i = (Integer)msg;
            if (i==limit) {
               //stop
            }
            else if (i==start) {
                collActor.send(i);
            } else if (i%start!=0) {
                if (!nextActor.isPresent()) {
                    nextActor = Optional.of(initActor(i, limit, collActor));
                }
                nextActor.ifPresent(a -> a.send(i));
            }
        }
    }

    public static final class CollectorActor extends Actor {
        private final Collection<Integer> collector = new ArrayList<Integer>();

        @Override
        public void process(final Object msg) {
            final int prime = (Integer) msg;
            collector.add(prime);
        }

        int size() {
            return collector.size();
        }
    }

}
