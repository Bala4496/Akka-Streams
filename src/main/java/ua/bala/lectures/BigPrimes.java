package ua.bala.lectures;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.Attributes;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class BigPrimes {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorSystem");

        Source<Integer, NotUsed> source = Source.range(1, 10);

        Flow<Integer, BigInteger, NotUsed> toBigIntegerFlow = Flow.of(Integer.class)
            .map(e -> {
                BigInteger result = new BigInteger(3000, new Random());
                System.out.println("Big integer : " + result);
                return result;
            });

        Flow<BigInteger, BigInteger, NotUsed> toPrime = Flow.of(BigInteger.class)
            .map(e -> {
                BigInteger prime = e.nextProbablePrime();
                System.out.println("Prime: " + prime);
                return prime;
            });

        Flow<BigInteger, BigInteger, NotUsed> toPrimeAsync = Flow.of(BigInteger.class)
            .mapAsyncUnordered(8, e -> {
                CompletableFuture<BigInteger> futurePrime = new CompletableFuture<>();
                futurePrime.completeAsync(() -> {
                    BigInteger prime = e.nextProbablePrime();
                    System.out.println("Prime: " + prime);
                    return prime;
                });
                return futurePrime;
            });

        Flow<BigInteger, List<BigInteger>, NotUsed> toListAndSort = Flow.of(BigInteger.class)
            .grouped(10)
            .map(list -> {
                List<BigInteger> out = new ArrayList<>(list);
                Collections.sort(out);
                return out;
            });

        Sink<List<BigInteger>, CompletionStage<Done>> print = Sink.foreach(System.out::println);

        CompletionStage<Done> result = source
            .via(toBigIntegerFlow)
//            .buffer(16, OverflowStrategy.backpressure())
            .async()
//            .via(toPrime.addAttributes(Attributes.inputBuffer(16, 32)))
            .via(toPrimeAsync)
            .async()
            .via(toListAndSort)
            .toMat(print, Keep.right())
            .run(actorSystem);

        result.whenComplete((value, throwable) -> {
            long eng = System.currentTimeMillis();
            System.out.println("Application ran in " + (eng - start) + "ms.");
            actorSystem.terminate();
        });
    }
}
