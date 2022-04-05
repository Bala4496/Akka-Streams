package ua.bala.lectures;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class SimpleStream {
    public static void main(String[] args) {
//        Sources
//        Finite Source
        Source<Integer, NotUsed> rangeSource = Source.range(1, 10, 2);
        Source<Integer, NotUsed> singleSource = Source.single(17);
        List<String> names = List.of("James", "Jill", "Sally", "Denise");
        Source<String, NotUsed> repeatingNamesSource = Source.from(names);

        //Infinite Source
        Source<Double, NotUsed> piSource = Source.repeat(3.141592654);
        Source<String, NotUsed> repeatingNames = Source.cycle(names::iterator);

        Iterator<Integer> integerIterator = Stream.iterate(0, i -> i + 1).iterator();
        Source<Integer, NotUsed> infiniteRangeSource = Source
            .fromIterator(() -> integerIterator)
            .throttle(1, Duration.ofSeconds(3))
            .take(5);

//        Flows
        Flow<Double, String, NotUsed> doubleFlow = Flow.of(Double.class).map(value -> "The text value is " + value);
        Flow<Integer, String, NotUsed> integerFlow = Flow.of(Integer.class).map(value -> "The text value is " + value);
        Flow<String, String, NotUsed> stringFlow = Flow.of(String.class).map(value -> "The text value is " + value);

//        Sinks
        Sink<String, CompletionStage<Done>> sink = Sink.foreach(System.out::println);
        Sink<String, CompletionStage<Done>> ignoreSink = Sink.ignore();

//        Running
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorSystem");
//        RunnableGraph<NotUsed> graph = infiniteRangeSource.via(integerFlow).to(sink);
//        graph.run(actorSystem);
        infiniteRangeSource.via(integerFlow).to(sink).run(actorSystem); // same as 2 lines above

//        sink.runWith(repeatingNamesSource, actorSystem);    //equivalent to repeatingNamesSource.to(sink).run(actorSystem);
//        sink.runWith(repeatingNamesSource.via(stringFlow), actorSystem);    //equivalent to repeatingNamesSource.via(stringFlow).to(sink).run(actorSystem);
//        repeatingNamesSource.via(stringFlow).runForeach(System.out::println, actorSystem);    //equivalent to repeatingNamesSource.via(stringFlow).to(sink).run(actorSystem);
    }
}
