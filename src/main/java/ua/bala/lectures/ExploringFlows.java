package ua.bala.lectures;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class ExploringFlows {
    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorName");

        Source<Integer, NotUsed> numbers = Source.range(1, 100);

        Flow<Integer, Integer, NotUsed> filterFlow = Flow.of(Integer.class).filter( x -> x % 17 == 0);
        Flow<Integer, Integer, NotUsed> mapConcatFlow = Flow.of(Integer.class).mapConcat( x -> {
            List<Integer> results = List.of(x, x+1, x+2);
            return results;
        });

        Flow<Integer, Integer, NotUsed> groupedFlow = Flow.of(Integer.class)
            .grouped(3)
            .map(x -> {
                List<Integer> newList = new ArrayList<>(x);
                newList.sort(Collections.reverseOrder());
                return newList;
            })
            .mapConcat(x -> x);

//        Not working
//        Flow<List<Integer>, Integer, NotUsed> ungroupedFlow = Flow.of(List.class).mapConcat(x -> x);

        Sink<Integer, CompletionStage<Done>> printSink = Sink.foreach(System.out::println);

        Flow<Integer, Integer, NotUsed> chainFlow = filterFlow.via(mapConcatFlow);

        numbers.via(chainFlow).via(groupedFlow).to(printSink).run(actorSystem);

    }
}
