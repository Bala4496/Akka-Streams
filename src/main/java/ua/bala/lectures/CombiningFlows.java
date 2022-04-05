package ua.bala.lectures;


import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class CombiningFlows {
    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorName");

        Source<String, NotUsed> sentencesSource = Source.from(List.of("The sky is blue", "The moon is only seen at night"));

        Flow<String, Integer, NotUsed> howManyWordsFlow = Flow.of(String.class)
            .map(s -> s.split(" ").length);

        Source<Integer, NotUsed> howManyWordsSource = sentencesSource.via(howManyWordsFlow);

        Source<Integer, NotUsed> sentencesSource2 = Source.from(List.of("The sky is blue", "The moon is only seen at night"))
            .map(s -> s.split(" ").length);

        Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        Sink<String, NotUsed> combinedSink = howManyWordsFlow.to(sink);

        howManyWordsSource.to(sink).run(actorSystem);

    }
}
