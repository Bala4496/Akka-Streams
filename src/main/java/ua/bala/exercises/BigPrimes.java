package ua.bala.exercises;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class BigPrimes {
    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorSystem");

        Source<Integer, NotUsed> integerSource = Source.range(1, 10);

        Flow<Integer, BigInteger, NotUsed> toBigIntegerFlow = Flow.of(Integer.class)
            .map(e -> new BigInteger(2000, new Random()));

        Flow<BigInteger, BigInteger, NotUsed> toPrime = Flow.of(BigInteger.class)
            .map(e -> {
                BigInteger prime = e.nextProbablePrime();
                System.out.println("Prime: " + e);
                return e;
            });

        Flow<BigInteger, List<BigInteger>, NotUsed> toListAndSort = Flow.of(BigInteger.class)
            .grouped(10)
            .map(list -> {
                List<BigInteger> out = new ArrayList<>(list);
                Collections.sort(out);
                return out;
            });

        Sink<List<BigInteger>, CompletionStage<Done>> print = Sink.foreach(System.out::println);

        integerSource
            .via(toBigIntegerFlow)
            .via(toPrime)
            .via(toListAndSort)
            .to(print)
            .run(actorSystem);

    }
}
