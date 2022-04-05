package ua.bala.exercises;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.*;
import akka.stream.javadsl.*;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Tracker {

    public static void main(String[] args) {
        Map<Integer, VehiclePositionMessage> vehicleTrackingMap = new HashMap<>();
        for (int i = 1; i <=8; i++)
            vehicleTrackingMap.put(i, new VehiclePositionMessage(1, new Date(), 0,0));

        ActorSystem<Object> actor = ActorSystem.create(Behaviors.empty(), "actor");

        Source<String, NotUsed> source = Source.repeat("Start!").throttle(1, Duration.ofSeconds(10));

        Flow<String, Integer, NotUsed> idsTransform = Flow.of(String.class).mapConcat(e -> List.of(1,2,3,4,5,6,7,8));
        Flow<Integer, VehiclePositionMessage, NotUsed> vehiclePosition = Flow.of(Integer.class).mapAsyncUnordered(8, id -> {
            CompletableFuture<VehiclePositionMessage> future = new CompletableFuture<>();
            UtilityFunctions utilityFunctions = new UtilityFunctions();
            future.completeAsync(() -> utilityFunctions.getVehiclePosition(id));
            return future;
        });
        Flow<VehiclePositionMessage, VehicleSpeed, NotUsed> vehicleSpeed = Flow.of(VehiclePositionMessage.class).map( vpm -> {
                UtilityFunctions utilityFunctions = new UtilityFunctions();
                VehiclePositionMessage prevVpm = vehicleTrackingMap.get(vpm.getVehicleId());
                VehicleSpeed speed = utilityFunctions.calculateSpeed(vpm, prevVpm);
                System.out.println("Vehicle " + vpm.getVehicleId() + " is travelling at " + speed.getSpeed());
                vehicleTrackingMap.put(vpm.getVehicleId(), vpm);
                return speed;
            });
        Flow<VehicleSpeed, VehicleSpeed, NotUsed> speedFilter = Flow.of(VehicleSpeed.class).filter(s -> s.getSpeed() > 95);

        Sink<VehicleSpeed, CompletionStage<VehicleSpeed>> sink = Sink.head();

        RunnableGraph<CompletionStage<VehicleSpeed>> runnableGraph = RunnableGraph.fromGraph(
            GraphDSL.create(sink, (builder, out) -> {

                SourceShape<String> sourceShape = builder.add(source);

                FlowShape<String, Integer> idsTransformShape = builder.add(idsTransform);
                FlowShape<VehiclePositionMessage, VehicleSpeed> vehicleSpeedShape = builder.add(vehicleSpeed);
                FlowShape<VehicleSpeed, VehicleSpeed> speedFilterShape = builder.add(speedFilter);

                UniformFanOutShape<Integer, Integer> balance = builder.add(Balance.create(8,true));

                UniformFanInShape<VehiclePositionMessage, VehiclePositionMessage> merge = builder.add(Merge.create(8));

                builder.from(sourceShape)
                    .via(idsTransformShape)
                    .viaFanOut(balance);

                for (int i = 0; i < 8; i++) {
                    builder.from(balance).via(builder.add(vehiclePosition.async())).toFanIn(merge);
                }

                builder.from(merge)
                    .via(vehicleSpeedShape)
                    .via(speedFilterShape)
                    .to(out);

                return ClosedShape.getInstance();
            })
        );

        CompletionStage<VehicleSpeed> result = runnableGraph.run(actor);

        result.whenComplete((value, throwable) -> {
            if (throwable != null)
                System.out.println("Something went wrong " + throwable);
            else
                System.out.println("Vehicle " + value.getVehicleId() + " was going at a speed as " + value.getSpeed());
            actor.terminate();
        });

//        CompletionStage<VehicleSpeed> graph = source
//            .via(idsTransform)
//            .async()
//            .via(vehiclePosition)
//            .async()
//            .via(vehicleSpeed)
//            .via(speedFilter)
//            .toMat(sink, Keep.right())
//            .run(actor);
//
//        graph.whenComplete((value, throwable) -> {
//            if (throwable != null)
//                System.out.println("Something went wrong " + throwable);
//            else
//                System.out.println("Vehicle " + value.getVehicleId() + " was going at a speed as " + value.getSpeed());
//            actor.terminate();
//        });
    }

}
